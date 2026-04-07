package future_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	future "github.com/llyb120/go-future"
)

func TestRootPackageRunsBundledWorkflowWithCustomSQLExecutor(t *testing.T) {
	catalog, err := future.LoadDir("workflows")
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	wf, ok := catalog.Get("customer-orders-external")
	if !ok || wf == nil {
		t.Fatalf("expected bundled external workflow")
	}

	executor := future.NewExecutorWithSQLExecutors(nil, map[string]future.SQLExecutor{
		"default": future.SQLExecutorFunc(rootTestSQLQuery),
	})

	result, err := executor.Run(context.Background(), wf, struct {
		CustomerEmail string
		OrderStatus   string
	}{
		CustomerEmail: "alice.future@demo.ai",
		OrderStatus:   "paid",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	viewValue, ok := result.GetScope("customerOrderView")
	view, ok := viewValue.(map[string]any)
	if !ok {
		t.Fatalf("expected customerOrderView map, got %T", viewValue)
	}

	customer, ok := view["customer"].(map[string]any)
	if !ok || customer["name"] != "Alice Future" {
		t.Fatalf("expected Alice Future, got %#v", view["customer"])
	}

	orders, ok := view["orders"].([]any)
	if !ok || len(orders) != 1 {
		t.Fatalf("expected one paid order, got %#v", view["orders"])
	}

	statsValue, ok := result.GetResult()
	stats, ok := statsValue.(map[string]any)
	if !ok {
		t.Fatalf("expected customerOrderStats map, got %T", statsValue)
	}
	if stats["customerName"] != "Alice Future" {
		t.Fatalf("expected customerName Alice Future, got %#v", stats["customerName"])
	}
	if stats["totalOrders"] != float64(1) {
		t.Fatalf("expected totalOrders 1, got %#v", stats["totalOrders"])
	}
	if stats["totalItems"] != float64(3) {
		t.Fatalf("expected totalItems 3, got %#v", stats["totalItems"])
	}
	if stats["totalAmount"] != float64(247) {
		t.Fatalf("expected totalAmount 247, got %#v", stats["totalAmount"])
	}
}

func TestRootPackageSupportsEmbeddedLoadersAndCustomSQLExecutor(t *testing.T) {
	fsys := fstest.MapFS{
		"workflows/simple.xml": {
			Data: []byte(`<workflow name="embedded-root" title="Embedded Root">
  <input name="tenant" required="true" />
  <sql name="rows" mode="query" datasource="hook"><![CDATA[
SELECT :tenant AS tenant;
  ]]></sql>
  <var name="tenantName" from="rows > :first > tenant" />
</workflow>`),
		},
	}

	catalog, err := future.LoadDirFS(fsys, "workflows")
	if err != nil {
		t.Fatalf("LoadDirFS() error = %v", err)
	}

	wf, ok := catalog.Get("embedded-root")
	if !ok {
		t.Fatalf("expected embedded-root workflow")
	}

	executor := future.NewExecutorWithSQLExecutors(nil, map[string]future.SQLExecutor{
		"hook": future.SQLExecutorFunc(func(ctx context.Context, request future.SQLRequest) ([]map[string]string, error) {
			_ = ctx
			if request.SQL != "SELECT :tenant AS tenant;" {
				t.Fatalf("unexpected SQL: %q", request.SQL)
			}
			if len(request.Args) != 1 {
				t.Fatalf("expected one argument, got %#v", request.Args)
			}
			named, ok := request.Args[0].(sql.NamedArg)
			if !ok || named.Name != "tenant" || named.Value != "acme" {
				t.Fatalf("expected named arg tenant=acme, got %#v", request.Args[0])
			}
			return []map[string]string{{"tenant": "acme"}}, nil
		}),
	})

	result, err := executor.Run(context.Background(), wf, map[string]any{
		"tenant": "acme",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got, ok := result.GetResult(); !ok || got != "acme" {
		t.Fatalf("expected tenantName=acme, got %#v", got)
	}
}

func rootTestSQLQuery(ctx context.Context, request future.SQLRequest) ([]map[string]string, error) {
	_ = ctx

	normalized := strings.ToLower(strings.Join(strings.Fields(request.SQL), " "))
	switch {
	case strings.Contains(normalized, "from customers"):
		email := strings.ToLower(rootNamedArg(request.Args, "customerEmailKey"))
		for _, row := range rootCustomerRows {
			if strings.ToLower(row["email"]) == email {
				return []map[string]string{cloneRootRow(row)}, nil
			}
		}
		return []map[string]string{}, nil

	case strings.Contains(normalized, "from orders"):
		customerID := rootPositionalInt(request.Args, 0)
		status := strings.ToLower(rootNamedArg(request.Args, "orderStatus"))
		if status == "" && len(rootPositionalArgs(request.Args)) > 1 {
			status = strings.ToLower(fmt.Sprint(rootPositionalArgs(request.Args)[1]))
		}

		rows := make([]map[string]string, 0)
		for _, row := range rootOrderRows {
			if parseRootInt(row["customer_id"]) != customerID {
				continue
			}
			if status != "" && strings.ToLower(row["status"]) != status {
				continue
			}
			rows = append(rows, cloneRootRow(row))
		}
		return rows, nil

	case strings.Contains(normalized, "from order_items"):
		if strings.Contains(normalized, "1 = 0") {
			return []map[string]string{}, nil
		}
		idSet := map[int64]struct{}{}
		for _, arg := range rootPositionalArgs(request.Args) {
			idSet[rootAnyToInt(arg)] = struct{}{}
		}
		rows := make([]map[string]string, 0)
		for _, row := range rootOrderItemRows {
			if _, ok := idSet[parseRootInt(row["order_id"])]; ok {
				rows = append(rows, cloneRootRow(row))
			}
		}
		return rows, nil

	case strings.Contains(normalized, "from products"):
		if strings.Contains(normalized, "1 = 0") {
			return []map[string]string{}, nil
		}
		idSet := map[int64]struct{}{}
		for _, arg := range rootPositionalArgs(request.Args) {
			idSet[rootAnyToInt(arg)] = struct{}{}
		}
		rows := make([]map[string]string, 0)
		for _, row := range rootProductRows {
			if _, ok := idSet[parseRootInt(row["id"])]; ok {
				rows = append(rows, cloneRootRow(row))
			}
		}
		return rows, nil
	}

	return nil, fmt.Errorf("unsupported root test query: %s", request.SQL)
}

func rootNamedArg(args []any, name string) string {
	for _, arg := range args {
		named, ok := arg.(sql.NamedArg)
		if ok && strings.EqualFold(named.Name, name) {
			return fmt.Sprint(named.Value)
		}
	}
	return ""
}

func rootPositionalArgs(args []any) []any {
	values := make([]any, 0, len(args))
	for _, arg := range args {
		if named, ok := arg.(sql.NamedArg); ok {
			values = append(values, named.Value)
			continue
		}
		values = append(values, arg)
	}
	return values
}

func rootPositionalInt(args []any, index int) int64 {
	values := rootPositionalArgs(args)
	if index < 0 || index >= len(values) {
		return 0
	}
	return rootAnyToInt(values[index])
}

func rootAnyToInt(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case int32:
		return int64(typed)
	case float64:
		return int64(typed)
	case string:
		return parseRootInt(typed)
	default:
		return parseRootInt(fmt.Sprint(value))
	}
}

func parseRootInt(value string) int64 {
	number, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return number
}

func cloneRootRow(row map[string]string) map[string]string {
	cloned := make(map[string]string, len(row))
	for key, value := range row {
		cloned[key] = value
	}
	return cloned
}

var rootCustomerRows = []map[string]string{
	{"id": "1", "name": "Alice Future", "email": "alice.future@demo.ai", "level": "gold"},
	{"id": "2", "name": "Bob Labs", "email": "bob.labs@demo.ai", "level": "silver"},
}

var rootProductRows = []map[string]string{
	{"id": "1", "name": "Copilot Seat", "price": "99"},
	{"id": "2", "name": "Workflow Engine", "price": "249"},
	{"id": "3", "name": "Knowledge Pack", "price": "49"},
}

var rootOrderRows = []map[string]string{
	{"id": "1", "customer_id": "1", "order_no": "SO-1001", "status": "paid", "created_at": "2026-04-01T10:00:00Z"},
	{"id": "2", "customer_id": "1", "order_no": "SO-1002", "status": "pending", "created_at": "2026-04-03T14:30:00Z"},
	{"id": "3", "customer_id": "2", "order_no": "SO-1003", "status": "paid", "created_at": "2026-04-04T09:15:00Z"},
}

var rootOrderItemRows = []map[string]string{
	{"id": "1", "order_id": "1", "product_id": "1", "quantity": "2", "unit_price": "99", "amount": "198"},
	{"id": "2", "order_id": "1", "product_id": "3", "quantity": "1", "unit_price": "49", "amount": "49"},
	{"id": "3", "order_id": "2", "product_id": "2", "quantity": "1", "unit_price": "249", "amount": "249"},
	{"id": "4", "order_id": "3", "product_id": "1", "quantity": "1", "unit_price": "99", "amount": "99"},
	{"id": "5", "order_id": "3", "product_id": "2", "quantity": "1", "unit_price": "249", "amount": "249"},
}
