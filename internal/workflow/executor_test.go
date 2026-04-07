package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestExecutorRunQueryWorkflow(t *testing.T) {
	wf := &Workflow{
		Name:  "user-search",
		Title: "User Search",
		Inputs: []Input{
			{Name: "tenant", Required: true},
			{Name: "keyword"},
			{Name: "limit", Default: "10"},
		},
		Steps: []Step{
			{
				Kind: "var",
				Name: "tenantCode",
				From: "tenant",
				Op:   "trim,upper",
			},
			{
				Kind:    "var",
				Name:    "keyword",
				From:    "keyword",
				Default: "",
				Op:      "trim",
			},
			{
				Kind:     "var",
				Name:     "keywordLike",
				Template: "%{{keyword}}%",
			},
			{
				Kind:    "var",
				Name:    "limitNum",
				From:    "limit",
				Default: "10",
				Op:      "trim,int",
			},
			{
				Kind: "sql",
				Mode: "query",
				Text: `
SELECT id, tenant, name, email, status
FROM users
WHERE tenant = :tenantCode
  AND (:keyword = '' OR name LIKE :keywordLike OR email LIKE :keywordLike)
ORDER BY id
LIMIT :limitNum;`,
			},
		},
	}

	executor := newTestExecutor(t)

	result, err := executor.Run(context.Background(), wf, struct {
		Tenant  string
		Keyword string
		Limit   int
	}{
		Tenant:  " acme ",
		Keyword: "Alice",
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Query == nil {
		t.Fatalf("expected query result, got nil")
	}

	if len(result.Query.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Query.Rows))
	}

	row := result.Query.Rows[0]
	if row["name"] != "Alice Zhang" {
		t.Fatalf("expected Alice Zhang, got %#v", row["name"])
	}

	if got, ok := findResolvedParam(result, "tenantCode"); !ok || got != "ACME" {
		t.Fatalf("expected tenantCode to be ACME, got %#v", got)
	}

	if got, ok := findResolvedParam(result, "limitNum"); !ok || got != int64(2) {
		t.Fatalf("expected limitNum to be 2, got %#v", got)
	}

	scopeValue, ok := result.GetScope("tenantCode")
	if !ok || scopeValue != "ACME" {
		t.Fatalf("expected scope tenantCode to be ACME, got %#v", scopeValue)
	}

	lastValue, ok := result.GetResult()
	if !ok || lastValue == nil {
		t.Fatalf("expected last result to exist, got %#v", lastValue)
	}
}

func TestLoadDirFSSupportsEmbeddedResources(t *testing.T) {
	fsys := fstest.MapFS{
		"workflows/report/report.xml": {
			Data: []byte(`<workflow name="embedded-report" title="Embedded Report">
  <input name="tenant" required="true" />
  <sql name="rows" mode="query" src="reports.by-tenant" />
  <transform name="summary" mode="js" from="rows > :first" entry="buildSummary" />
</workflow>`),
		},
		"workflows/report/res/reports.sql.md": {
			Data: []byte(`# reports

## by-tenant
` + "```sql" + `
SELECT :tenant AS tenant, '1' AS value;
` + "```"),
		},
		"workflows/report/res/summary.js": {
			Data: []byte(`export function buildSummary({ input }) {
  return {
    tenant: input?.tenant || "",
    value: input?.value || "",
  };
}`),
		},
	}

	catalog, err := LoadDirFS(fsys, "workflows")
	if err != nil {
		t.Fatalf("LoadDirFS() error = %v", err)
	}

	wf, ok := catalog.Get("embedded-report")
	if !ok {
		t.Fatalf("expected embedded-report workflow")
	}

	executor := NewExecutorWithSQLExecutors(nil, map[string]SQLExecutor{
		"default": SQLExecutorFunc(func(ctx context.Context, request SQLRequest) ([]map[string]string, error) {
			if request.SQL != "SELECT :tenant AS tenant, '1' AS value;" {
				t.Fatalf("unexpected SQL: %q", request.SQL)
			}
			if len(request.Args) != 1 {
				t.Fatalf("expected one argument, got %#v", request.Args)
			}
			named, ok := request.Args[0].(sql.NamedArg)
			if !ok || named.Name != "tenant" || named.Value != "acme" {
				t.Fatalf("expected named arg tenant=acme, got %#v", request.Args[0])
			}
			return []map[string]string{
				{"tenant": "acme", "value": "1"},
			}, nil
		}),
	})

	result, err := executor.Run(context.Background(), wf, map[string]string{
		"tenant": "acme",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	summaryValue, ok := findResolvedParam(result, "summary")
	if !ok {
		t.Fatalf("expected summary result")
	}

	summary, ok := summaryValue.(map[string]any)
	if !ok {
		t.Fatalf("expected summary map, got %T", summaryValue)
	}
	if summary["tenant"] != "acme" {
		t.Fatalf("expected tenant=acme, got %#v", summary["tenant"])
	}
	if summary["value"] != float64(1) {
		t.Fatalf("expected value=1, got %#v", summary["value"])
	}
}

func TestExecutorUsesCustomSQLExecutor(t *testing.T) {
	wf := &Workflow{
		Name:  "custom-sql",
		Title: "Custom SQL",
		Inputs: []Input{
			{Name: "tenant", Required: true},
		},
		Steps: []Step{
			{Kind: "sql", Name: "rows", Mode: "query", Datasource: "framework", Text: `SELECT :tenant AS tenant;`},
			{Kind: "var", Name: "tenantName", From: "rows > :first > tenant"},
		},
	}

	called := 0
	executor := NewExecutorWithSQLExecutors(nil, map[string]SQLExecutor{
		"framework": SQLExecutorFunc(func(ctx context.Context, request SQLRequest) ([]map[string]string, error) {
			called++
			if request.Datasource != "framework" {
				t.Fatalf("expected datasource framework, got %q", request.Datasource)
			}
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
			return []map[string]string{
				{"tenant": "acme"},
			}, nil
		}),
	})

	result, err := executor.Run(context.Background(), wf, map[string]string{
		"tenant": "acme",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if called != 1 {
		t.Fatalf("expected custom SQL executor to be called once, got %d", called)
	}
	if got, ok := findResolvedParam(result, "tenantName"); !ok || got != "acme" {
		t.Fatalf("expected tenantName=acme, got %#v", got)
	}
}

func TestSelectValueSupportsDomStyleSelectors(t *testing.T) {
	data := map[string]any{
		"catalog": map[string]any{
			"orders": []any{
				map[string]any{
					"id":     1,
					"status": "paid",
					"items": []any{
						map[string]any{
							"quantity": 2,
							"product": map[string]any{
								"id":   1,
								"name": "Copilot Seat",
								"sku":  "seat",
							},
						},
						map[string]any{
							"quantity": 1,
							"product": map[string]any{
								"id":   2,
								"name": "Workflow Engine",
								"sku":  "engine",
							},
						},
					},
				},
				map[string]any{
					"id":     2,
					"status": "pending",
					"items":  []any{},
				},
			},
			"productById": map[string]any{
				"1": map[string]any{"name": "Copilot Seat"},
				"2": map[string]any{"name": "Workflow Engine"},
			},
		},
	}

	keysValue, found, err := selectValue(data, "catalog > orders[status=paid] > items > :first > product:keys")
	if err != nil {
		t.Fatalf("selectValue() error = %v", err)
	}
	if !found {
		t.Fatalf("expected keys selector to find a result")
	}

	keys, ok := keysValue.([]string)
	if !ok {
		t.Fatalf("expected keys selector to return []string, got %T", keysValue)
	}
	if !reflect.DeepEqual(keys, []string{"id", "name", "sku"}) {
		t.Fatalf("unexpected keys: %#v", keys)
	}

	statusValue, found, err := selectValue(data, "catalog > orders > :eq(1) > status")
	if err != nil {
		t.Fatalf("selectValue() error = %v", err)
	}
	if !found || statusValue != "pending" {
		t.Fatalf("expected pending status, got %#v", statusValue)
	}

	productKeysValue, found, err := selectValue(data, "catalog > productById:keys")
	if err != nil {
		t.Fatalf("selectValue() error = %v", err)
	}
	if !found {
		t.Fatalf("expected productById:keys to find a result")
	}

	productKeys, ok := productKeysValue.([]string)
	if !ok {
		t.Fatalf("expected productById:keys to return []string, got %T", productKeysValue)
	}
	if !reflect.DeepEqual(productKeys, []string{"1", "2"}) {
		t.Fatalf("unexpected product keys: %#v", productKeys)
	}

	descendantValue, found, err := selectValue(data, "catalog items product name")
	if err != nil {
		t.Fatalf("selectValue() descendant error = %v", err)
	}
	if !found {
		t.Fatalf("expected descendant selector to find products")
	}

	descendantNames, ok := descendantValue.([]any)
	if !ok || !reflect.DeepEqual(descendantNames, []any{"Copilot Seat", "Workflow Engine"}) {
		t.Fatalf("unexpected descendant names: %#v", descendantValue)
	}

	directOnlyValue, found, err := selectValue(data, "catalog > items")
	if err != nil {
		t.Fatalf("selectValue() direct child error = %v", err)
	}
	if found || directOnlyValue != nil {
		t.Fatalf("expected direct child selector to miss nested items, got %#v", directOnlyValue)
	}
}

func TestExecutorRunGoSQLWorkflowWithComplexInput(t *testing.T) {
	wf := &Workflow{
		Name:  "user-search-advanced",
		Title: "Advanced User Search",
		Inputs: []Input{
			{Name: "payload", Type: "json", Required: true},
		},
		Steps: []Step{
			{
				Kind:   "transform",
				Name:   "queryArgs",
				From:   "payload",
				Export: true,
				Children: []Step{
					{Kind: "field", Path: "tenantCode", From: "tenant", Op: "trim,upper"},
					{Kind: "field", Path: "keyword", From: "filters > keyword", Optional: true, Default: "", Op: "trim"},
					{Kind: "field", Path: "statusList", From: "filters > statuses[*]", Optional: true, Default: "[]", Op: "json"},
					{Kind: "field", Path: "limitNum", From: "page > limit", Optional: true, Default: "10", Op: "int"},
					{Kind: "field", Path: "offsetNum", From: "page > offset", Optional: true, Default: "0", Op: "int"},
					{Kind: "field", Path: "sortColumn", From: "page > sort > column", Optional: true, Default: "id", Op: "trim,lower,allow(id|name|email|status)"},
					{Kind: "field", Path: "sortDirection", From: "page > sort > direction", Optional: true, Default: "desc", Op: "trim,lower,allow(asc|desc)"},
				},
			},
			{
				Kind:   "sql",
				Mode:   "query",
				Engine: "gosql",
				Text: `
select id, tenant, name, email, status
from users
where tenant = @tenantCode
@if keyword != "" {
  and (name like '%' || @keyword || '%' or email like '%' || @keyword || '%')
}
@if len(statusList) > 0 {
  and status in (@statusList)
}
order by @=sortColumn @=sortDirection
limit @limitNum
offset @offsetNum`,
			},
		},
	}

	executor := newTestExecutor(t)

	result, err := executor.Run(context.Background(), wf, map[string]any{
		"tenant": "acme",
		"filters": map[string]any{
			"keyword":  "alice",
			"statuses": []string{"active"},
		},
		"page": map[string]any{
			"limit":  5,
			"offset": 0,
			"sort": map[string]any{
				"column":    "id",
				"direction": "desc",
			},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Query == nil {
		t.Fatalf("expected query result, got nil")
	}

	if len(result.Query.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Query.Rows))
	}

	if !strings.Contains(strings.ToLower(result.SQL), "status in") {
		t.Fatalf("expected rendered SQL to include status filter, got %q", result.SQL)
	}

	queryArgs, ok := findResolvedParam(result, "queryArgs")
	if !ok {
		t.Fatalf("expected queryArgs to exist")
	}

	queryArgsMap, ok := queryArgs.(map[string]any)
	if !ok {
		t.Fatalf("expected queryArgs to be a map, got %T", queryArgs)
	}

	if queryArgsMap["tenantCode"] != "ACME" {
		t.Fatalf("expected queryArgs.tenantCode to be ACME, got %#v", queryArgsMap["tenantCode"])
	}

	statuses, ok := findResolvedParam(result, "statusList")
	if !ok {
		t.Fatalf("expected statusList to exist")
	}

	statusSlice, ok := statuses.([]any)
	if !ok || len(statusSlice) != 1 || statusSlice[0] != "active" {
		t.Fatalf("expected statusList to contain active, got %#v", statuses)
	}
}

func TestExecutorRunAcceptsStructAsSingleJSONInput(t *testing.T) {
	wf := &Workflow{
		Name: "json-payload",
		Inputs: []Input{
			{Name: "payload", Type: "json", Required: true},
		},
		Steps: []Step{
			{Kind: "var", Name: "tenantCode", From: "payload > tenant", Op: "trim,upper"},
		},
	}

	executor := newTestExecutor(t)
	result, err := executor.Run(context.Background(), wf, struct {
		Tenant string
	}{
		Tenant: "acme",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got, ok := findResolvedParam(result, "tenantCode"); !ok || got != "ACME" {
		t.Fatalf("expected tenantCode to be ACME, got %#v", got)
	}
}

func TestExecutionGetResultPrefersAtResult(t *testing.T) {
	wf := &Workflow{
		Name: "explicit-result",
		Steps: []Step{
			{Kind: "var", Name: "first", Value: "first"},
			{Kind: "var", Name: "@result", Value: "picked"},
			{Kind: "var", Name: "last", Value: "last"},
		},
	}

	executor := newTestExecutor(t)
	result, err := executor.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	value, ok := result.GetScope("@result")
	if !ok || value != "picked" {
		t.Fatalf("expected scope @result to be picked, got %#v", value)
	}

	finalValue, ok := result.GetResult()
	if !ok || finalValue != "picked" {
		t.Fatalf("expected GetResult() to prefer @result, got %#v", finalValue)
	}
}

func TestExecutorBuildsTreeFromSQLRows(t *testing.T) {
	wf := &Workflow{
		Name:  "category-tree",
		Title: "Category Tree",
		Steps: []Step{
			{
				Kind: "sql",
				Name: "rows",
				Mode: "query",
				Text: `
SELECT id, parent_id, name
FROM categories
ORDER BY id;`,
			},
			{
				Kind:        "transform",
				Name:        "categoryTree",
				From:        "rows",
				Mode:        "tree",
				IDField:     "id",
				ParentField: "parent_id",
				ChildrenKey: "children",
				Root:        "0",
			},
		},
	}

	executor := newTestExecutor(t)
	result, err := executor.Run(context.Background(), wf, map[string]string{})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	treeValue, ok := findResolvedParam(result, "categoryTree")
	if !ok {
		t.Fatalf("expected categoryTree to be resolved")
	}

	tree, ok := treeValue.([]any)
	if !ok || len(tree) == 0 {
		t.Fatalf("expected non-empty tree, got %#v", treeValue)
	}

	rootNode, ok := tree[0].(map[string]any)
	if !ok {
		t.Fatalf("expected root node map, got %T", tree[0])
	}

	if rootNode["name"] != "AI" {
		t.Fatalf("expected root node AI, got %#v", rootNode["name"])
	}

	children, ok := rootNode["children"].([]any)
	if !ok || len(children) == 0 {
		t.Fatalf("expected root children, got %#v", rootNode["children"])
	}
}

func TestExecutorBuildsStructuredOrderViewFromRelatedTables(t *testing.T) {
	wf := &Workflow{
		Name:  "customer-orders-structured",
		Title: "Customer Orders Structured",
		Inputs: []Input{
			{Name: "customerEmail", Default: "alice.future@demo.ai"},
			{Name: "orderStatus", Default: ""},
		},
		Steps: []Step{
			{Kind: "var", Name: "customerEmailKey", From: "customerEmail", Default: "alice.future@demo.ai", Op: "trim,lower"},
			{Kind: "var", Name: "orderStatus", From: "orderStatus", Default: "", Op: "trim,lower"},
			{
				Kind: "sql",
				Name: "customerRows",
				Mode: "query",
				Text: `
SELECT id, name, email, level
FROM customers
WHERE lower(email) = :customerEmailKey;`,
			},
			{Kind: "var", Name: "customerId", From: "customerRows > :first > id", Op: "int"},
			{
				Kind:   "sql",
				Name:   "orders",
				Mode:   "query",
				Engine: "gosql",
				Text: `
select id, customer_id, order_no, status, created_at
from orders
where customer_id = @customerId
@if orderStatus != "" {
  and status = @orderStatus
}
order by id`,
			},
			{Kind: "var", Name: "orderIds", From: "orders > id", Optional: true, Default: "[]", Op: "json"},
			{
				Kind:   "sql",
				Name:   "orderItems",
				Mode:   "query",
				Engine: "gosql",
				Text: `
SELECT id, order_id, product_id, quantity, unit_price, quantity * unit_price AS amount
FROM order_items
where
@if len(orderIds) > 0 {
  order_id in (@orderIds)
} else {
  1 = 0
}
ORDER BY id;`,
			},
			{Kind: "var", Name: "productIds", From: "orderItems > product_id", Optional: true, Default: "[]", Op: "json"},
			{
				Kind:   "sql",
				Name:   "products",
				Mode:   "query",
				Engine: "gosql",
				Text: `
SELECT id, name, price
FROM products
where
@if len(productIds) > 0 {
  id in (@productIds)
} else {
  1 = 0
}
ORDER BY id;`,
			},
			{Kind: "transform", Name: "productById", From: "products", Mode: "index", By: "id"},
			{
				Kind: "transform",
				Name: "itemsWithProduct",
				From: "orderItems",
				Mode: "map",
				Children: []Step{
					{Kind: "field", Path: "id", From: "id"},
					{Kind: "field", Path: "orderId", From: "order_id"},
					{Kind: "field", Path: "quantity", From: "quantity"},
					{Kind: "field", Path: "unitPrice", From: "unit_price"},
					{Kind: "field", Path: "amount", From: "amount"},
					{Kind: "field", Path: "product", From: "productById > {{product_id}}"},
				},
			},
			{Kind: "transform", Name: "itemsByOrder", From: "itemsWithProduct", Mode: "group", By: "orderId"},
			{
				Kind: "transform",
				Name: "ordersView",
				From: "orders",
				Mode: "map",
				Children: []Step{
					{Kind: "field", Path: "id", From: "id"},
					{Kind: "field", Path: "orderNo", From: "order_no"},
					{Kind: "field", Path: "status", From: "status"},
					{Kind: "field", Path: "createdAt", From: "created_at"},
					{Kind: "field", Path: "items", From: "itemsByOrder > {{id}}", Optional: true, Default: "[]", Op: "json"},
				},
			},
			{
				Kind: "transform",
				Name: "customerOrderView",
				Children: []Step{
					{Kind: "field", Path: "customer", From: "customerRows > :first"},
					{Kind: "field", Path: "orders", From: "ordersView"},
				},
			},
			{
				Kind: "transform",
				Name: "customerOrderStats",
				Mode: "js",
				From: "customerOrderView",
				Text: `
const view = input || { customer: {}, orders: [] };
const orders = Array.isArray(view.orders) ? view.orders : [];
const items = orders.flatMap((order) => Array.isArray(order.items) ? order.items : []);

return {
  customerName: view.customer?.name || "",
  totalOrders: orders.length,
  totalItems: items.reduce((sum, item) => sum + (item.quantity || 0), 0),
  totalAmount: items.reduce((sum, item) => sum + (item.amount || 0), 0),
  rootKeys: await keys(view),
};`,
			},
		},
	}

	executor := newTestExecutor(t)
	result, err := executor.Run(context.Background(), wf, map[string]string{
		"customerEmail": "alice.future@demo.ai",
		"orderStatus":   "paid",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	viewValue, ok := findResolvedParam(result, "customerOrderView")
	if !ok {
		t.Fatalf("expected customerOrderView to be resolved")
	}

	viewMap, ok := viewValue.(map[string]any)
	if !ok {
		t.Fatalf("expected structured view map, got %T", viewValue)
	}

	customer, ok := viewMap["customer"].(map[string]any)
	if !ok || customer["name"] != "Alice Future" {
		t.Fatalf("expected customer Alice Future, got %#v", viewMap["customer"])
	}

	orders, ok := viewMap["orders"].([]any)
	if !ok || len(orders) != 1 {
		t.Fatalf("expected one paid order, got %#v", viewMap["orders"])
	}

	order, ok := orders[0].(map[string]any)
	if !ok || order["orderNo"] != "SO-1001" {
		t.Fatalf("expected order SO-1001, got %#v", orders[0])
	}

	items, ok := order["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected two order items, got %#v", order["items"])
	}

	firstItem, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first item map, got %#v", items[0])
	}

	product, ok := firstItem["product"].(map[string]any)
	if !ok || product["name"] != "Copilot Seat" {
		t.Fatalf("expected first product Copilot Seat, got %#v", firstItem["product"])
	}

	statsValue, ok := findResolvedParam(result, "customerOrderStats")
	if !ok {
		t.Fatalf("expected customerOrderStats to be resolved")
	}

	stats, ok := statsValue.(map[string]any)
	if !ok {
		t.Fatalf("expected customerOrderStats map, got %T", statsValue)
	}

	if stats["totalOrders"] != float64(1) {
		t.Fatalf("expected totalOrders 1, got %#v", stats["totalOrders"])
	}

	rootKeys, ok := stats["rootKeys"].([]any)
	if !ok || len(rootKeys) != 2 {
		t.Fatalf("expected root keys from js pick, got %#v", stats["rootKeys"])
	}
}

func TestExecutorRunExecWorkflow(t *testing.T) {
	db := newExecTestDB(t)

	wf := &Workflow{
		Name: "activate-user",
		Inputs: []Input{
			{Name: "tenant", Required: true},
			{Name: "email", Required: true},
		},
		Steps: []Step{
			{
				Kind: "var",
				Name: "tenantCode",
				From: "tenant",
				Op:   "trim,upper",
			},
			{
				Kind: "var",
				Name: "emailKey",
				From: "email",
				Op:   "trim,lower",
			},
			{
				Kind:  "transform",
				Name:  "statusValue",
				Value: "active",
			},
			{
				Kind: "sql",
				Mode: "exec",
				Text: `
UPDATE users
SET status = :statusValue
WHERE tenant = :tenantCode
  AND email = :emailKey;`,
			},
		},
	}

	executor := NewExecutor(map[string]*sql.DB{"default": db})

	result, err := executor.Run(context.Background(), wf, map[string]string{
		"tenant": "labs",
		"email":  "CAROL@labs.ai",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Exec == nil {
		t.Fatalf("expected exec result, got nil")
	}

	if result.Exec.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", result.Exec.RowsAffected)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM users WHERE email = 'carol@labs.ai'`).Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}

	if status != "active" {
		t.Fatalf("expected status active, got %q", status)
	}
}

func TestExecutorRunSQLFromMarkdownSource(t *testing.T) {
	dir := t.TempDir()

	workflowXML := `<workflow name="external-sql" title="External SQL">
  <input name="tenant" required="true" />
  <input name="keyword" default="" />

  <var name="tenantCode" from="tenant" op="trim,upper" />
  <var name="keyword" from="keyword" default="" op="trim" />
  <var name="keywordLike" template="%{{keyword}}%" />

  <sql name="users" mode="query" engine="gosql" src="snippets.active-users" />
</workflow>`

	markdown := `# snippets

## active-users

` + "```sql" + `
select id, tenant, name, email, status
from users
where tenant = @tenantCode
@if keyword != "" {
  and (name like @keywordLike or email like @keywordLike)
}
order by id
` + "```" + `
`

	workflowPath := filepath.Join(dir, "external-sql.xml")
	markdownPath := filepath.Join(dir, "res", "queries.sql.md")

	if err := os.MkdirAll(filepath.Dir(markdownPath), 0o755); err != nil {
		t.Fatalf("mkdir snippets: %v", err)
	}
	if err := os.WriteFile(workflowPath, []byte(workflowXML), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	if err := os.WriteFile(markdownPath, []byte(markdown), 0o644); err != nil {
		t.Fatalf("write markdown: %v", err)
	}

	catalog, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	wf, ok := catalog.Get("external-sql")
	if !ok {
		t.Fatalf("expected workflow external-sql")
	}

	executor := newTestExecutor(t)
	result, err := executor.Run(context.Background(), wf, map[string]string{
		"tenant":  "acme",
		"keyword": "alice",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Query == nil || len(result.Query.Rows) != 1 {
		t.Fatalf("expected 1 query row, got %#v", result.Query)
	}

	if got := result.Query.Rows[0]["name"]; got != "Alice Zhang" {
		t.Fatalf("expected Alice Zhang, got %#v", got)
	}
}

func TestExecutorRunSQLNamedSourceIgnoresPlainMarkdown(t *testing.T) {
	dir := t.TempDir()

	workflowXML := `<workflow name="external-sql" title="External SQL">
  <input name="tenant" required="true" />
  <sql name="users" mode="query" engine="gosql" src="snippets.active-users" />
</workflow>`

	markdown := `# snippets

## active-users

` + "```sql" + `
select id, tenant, name, email, status
from users
where tenant = @tenant
order by id
` + "```" + `
`

	workflowPath := filepath.Join(dir, "report.xml")
	markdownPath := filepath.Join(dir, "res", "queries.md")

	if err := os.MkdirAll(filepath.Dir(markdownPath), 0o755); err != nil {
		t.Fatalf("mkdir res: %v", err)
	}
	if err := os.WriteFile(workflowPath, []byte(workflowXML), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	if err := os.WriteFile(markdownPath, []byte(markdown), 0o644); err != nil {
		t.Fatalf("write markdown: %v", err)
	}

	_, err := LoadDir(dir)
	if err == nil {
		t.Fatalf("expected LoadDir() to reject plain markdown preload")
	}
	if !strings.Contains(err.Error(), `sql source "snippets.active-users" not found in preloaded markdown resources`) {
		t.Fatalf("expected named sql preload error, got %v", err)
	}
}

func TestExecutorRunPreloadedJSFunction(t *testing.T) {
	dir := t.TempDir()

	workflowXML := `<workflow name="external-js" title="External JS">
  <input name="customerEmail" default="alice.future@demo.ai" />

  <var name="customerEmailKey" from="customerEmail" default="alice.future@demo.ai" op="trim,lower" />

  <sql name="customerRows" mode="query"><![CDATA[
SELECT id, name, email
FROM customers
WHERE lower(email) = :customerEmailKey;
  ]]></sql>

  <transform name="customerSummary" mode="js" from="customerRows > :first" entry="customerSummary" />
</workflow>`

	script := `export function selectedEmail({ input }) {
  return input?.email || "";
}

export async function customerSummary({ input, keys }) {
  return {
    name: input?.name || "",
    email: selectedEmail({ input }),
    keys: await keys(input),
  };
}`

	workflowPath := filepath.Join(dir, "external-js.xml")
	scriptPath := filepath.Join(dir, "scripts", "customer-summary.js")

	if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
		t.Fatalf("mkdir scripts: %v", err)
	}
	if err := os.WriteFile(workflowPath, []byte(workflowXML), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	if err := os.WriteFile(scriptPath, []byte(script), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	catalog, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	wf, ok := catalog.Get("external-js")
	if !ok {
		t.Fatalf("expected workflow external-js")
	}

	executor := newTestExecutor(t)
	result, err := executor.Run(context.Background(), wf, map[string]string{
		"customerEmail": "alice.future@demo.ai",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	value, ok := findResolvedParam(result, "customerSummary")
	if !ok {
		t.Fatalf("expected customerSummary resolved param")
	}

	summary, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected summary map, got %T", value)
	}

	if summary["name"] != "Alice Future" {
		t.Fatalf("expected summary name Alice Future, got %#v", summary["name"])
	}

	keys, ok := summary["keys"].([]any)
	if !ok || len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %#v", summary["keys"])
	}
}

func TestLoadDirRejectsUnknownPreloadedJSEntry(t *testing.T) {
	dir := t.TempDir()

	workflowXML := `<workflow name="bad-external-js" title="Bad JS Source">
  <transform name="summary" mode="js" entry="missingSummary" />
</workflow>`

	script := `export function first() {
  return { ok: true };
}

export function second() {
  return { ok: false };
}`

	workflowPath := filepath.Join(dir, "bad-external-js.xml")
	scriptPath := filepath.Join(dir, "scripts", "summary.js")

	if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
		t.Fatalf("mkdir scripts: %v", err)
	}
	if err := os.WriteFile(workflowPath, []byte(workflowXML), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	if err := os.WriteFile(scriptPath, []byte(script), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	_, err := LoadDir(dir)
	if err == nil {
		t.Fatalf("expected LoadDir() to reject unknown preloaded js entry")
	}
	if !strings.Contains(err.Error(), "not found in preloaded javascript resources") {
		t.Fatalf("expected unknown preloaded javascript error, got %v", err)
	}
}

func TestLoadDirRejectsNonMarkdownExternalSQL(t *testing.T) {
	dir := t.TempDir()

	workflowXML := `<workflow name="bad-external-sql" title="Bad SQL Source">
  <sql name="users" mode="query" src="snippets\queries.sql" />
</workflow>`

	sqlText := `select id, name from users;`

	workflowPath := filepath.Join(dir, "bad-external-sql.xml")
	sqlPath := filepath.Join(dir, "snippets", "queries.sql")

	if err := os.MkdirAll(filepath.Dir(sqlPath), 0o755); err != nil {
		t.Fatalf("mkdir snippets: %v", err)
	}
	if err := os.WriteFile(workflowPath, []byte(workflowXML), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	if err := os.WriteFile(sqlPath, []byte(sqlText), 0o644); err != nil {
		t.Fatalf("write sql file: %v", err)
	}

	_, err := LoadDir(dir)
	if err == nil {
		t.Fatalf("expected LoadDir() to reject non-markdown sql source")
	}
	if !strings.Contains(err.Error(), "must be a markdown file") {
		t.Fatalf("expected markdown source error, got %v", err)
	}
}

func TestLoadDirSupportsNestedWorkflowFolders(t *testing.T) {
	dir := t.TempDir()

	alphaWorkflow := `<workflow name="alpha-report" title="Alpha Report">
  <sql name="rows" mode="query" src="alpha.get" />
  <transform name="summary" mode="js" from="rows > :first" entry="buildSummary" />
</workflow>`
	alphaSQL := `# alpha

## get
` + "```sql" + `
SELECT 1 AS value;
` + "```"
	alphaJS := `export function buildSummary({ input }) {
  return {
    source: "alpha",
    value: input?.value || 0,
  };
}`

	betaWorkflow := `<workflow name="beta-report" title="Beta Report">
  <sql name="rows" mode="query" src="beta.get" />
  <transform name="summary" mode="js" from="rows > :first" entry="buildSummary" />
</workflow>`
	betaSQL := `# beta

## get
` + "```sql" + `
SELECT 2 AS value;
` + "```"
	betaJS := `export function buildSummary({ input }) {
  return {
    source: "beta",
    value: input?.value || 0,
  };
}`

	alphaDir := filepath.Join(dir, "alpha")
	betaDir := filepath.Join(dir, "team", "beta")
	for _, item := range []struct {
		path    string
		content string
	}{
		{filepath.Join(alphaDir, "report.xml"), alphaWorkflow},
		{filepath.Join(alphaDir, "res", "queries.sql.md"), alphaSQL},
		{filepath.Join(alphaDir, "res", "summary.js"), alphaJS},
		{filepath.Join(betaDir, "date.xml"), betaWorkflow},
		{filepath.Join(betaDir, "res", "queries.sql.md"), betaSQL},
		{filepath.Join(betaDir, "res", "summary.js"), betaJS},
	} {
		if err := os.MkdirAll(filepath.Dir(item.path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(item.path), err)
		}
		if err := os.WriteFile(item.path, []byte(item.content), 0o644); err != nil {
			t.Fatalf("write %s: %v", item.path, err)
		}
	}

	catalog, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	executor := newTestExecutor(t)
	for _, expected := range []struct {
		name   string
		source string
		value  int64
	}{
		{name: "alpha-report", source: "alpha", value: 1},
		{name: "beta-report", source: "beta", value: 2},
	} {
		wf, ok := catalog.Get(expected.name)
		if !ok {
			t.Fatalf("expected workflow %s", expected.name)
		}

		result, err := executor.Run(context.Background(), wf, map[string]string{})
		if err != nil {
			t.Fatalf("Run(%s) error = %v", expected.name, err)
		}

		summaryValue, ok := findResolvedParam(result, "summary")
		if !ok {
			t.Fatalf("expected summary for %s", expected.name)
		}

		summary, ok := summaryValue.(map[string]any)
		if !ok {
			t.Fatalf("expected summary map for %s, got %T", expected.name, summaryValue)
		}

		if summary["source"] != expected.source {
			t.Fatalf("expected %s source %q, got %#v", expected.name, expected.source, summary["source"])
		}
		if fmt.Sprint(summary["value"]) != fmt.Sprint(expected.value) {
			t.Fatalf("expected %s value %d, got %#v", expected.name, expected.value, summary["value"])
		}
	}
}

func TestLoadDirSharesResFolderAcrossWorkflowXMLInSameDirectory(t *testing.T) {
	dir := t.TempDir()

	reportWorkflow := `<workflow name="folder-report" title="Folder Report">
  <sql name="rows" mode="query" src="folder.get-report" />
  <transform name="summary" mode="js" from="rows > :first" entry="buildSummary" />
</workflow>`
	dateWorkflow := `<workflow name="folder-dates" title="Folder Dates">
  <sql name="rows" mode="query" src="folder.get-dates" />
  <transform name="summary" mode="js" from="rows > :first" entry="buildSummary" />
</workflow>`
	sqlText := `# folder

## get-report
` + "```sql" + `
SELECT 7 AS value;
` + "```" + `

## get-dates
` + "```sql" + `
SELECT 9 AS value;
` + "```"
	jsText := `export function buildSummary({ input }) {
  return {
    value: input?.value || 0,
  };
}`

	workflowDir := filepath.Join(dir, "global_daily_report")
	for _, item := range []struct {
		path    string
		content string
	}{
		{filepath.Join(workflowDir, "report.xml"), reportWorkflow},
		{filepath.Join(workflowDir, "date.xml"), dateWorkflow},
		{filepath.Join(workflowDir, "res", "queries.sql.md"), sqlText},
		{filepath.Join(workflowDir, "res", "helpers.js"), jsText},
	} {
		if err := os.MkdirAll(filepath.Dir(item.path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(item.path), err)
		}
		if err := os.WriteFile(item.path, []byte(item.content), 0o644); err != nil {
			t.Fatalf("write %s: %v", item.path, err)
		}
	}

	catalog, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	executor := newTestExecutor(t)
	for _, expected := range []struct {
		name  string
		value int64
	}{
		{name: "folder-report", value: 7},
		{name: "folder-dates", value: 9},
	} {
		wf, ok := catalog.Get(expected.name)
		if !ok {
			t.Fatalf("expected workflow %s", expected.name)
		}

		result, err := executor.Run(context.Background(), wf, map[string]string{})
		if err != nil {
			t.Fatalf("Run(%s) error = %v", expected.name, err)
		}

		summaryValue, ok := findResolvedParam(result, "summary")
		if !ok {
			t.Fatalf("expected summary for %s", expected.name)
		}

		summary, ok := summaryValue.(map[string]any)
		if !ok {
			t.Fatalf("expected summary map for %s, got %T", expected.name, summaryValue)
		}

		if fmt.Sprint(summary["value"]) != fmt.Sprint(expected.value) {
			t.Fatalf("expected %s value %d, got %#v", expected.name, expected.value, summary["value"])
		}
	}
}

func findResolvedParam(result *Execution, name string) (any, bool) {
	for _, item := range result.Resolved {
		if item.Name == name {
			return item.Value, true
		}
	}
	return nil, false
}
