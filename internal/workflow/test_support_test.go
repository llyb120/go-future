package workflow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

var (
	execTestDriverOnce sync.Once
	execTestDBSeq      atomic.Uint64
	execTestDBStates   sync.Map
)

func newTestExecutor(t *testing.T) *Executor {
	t.Helper()

	return NewExecutorWithSQLExecutors(nil, map[string]SQLExecutor{
		"default": SQLExecutorFunc(func(ctx context.Context, request SQLRequest) ([]map[string]string, error) {
			_ = ctx
			return runTestQuery(request)
		}),
	})
}

func runTestQuery(request SQLRequest) ([]map[string]string, error) {
	normalized := normalizeTestSQL(request.SQL)
	switch {
	case strings.Contains(normalized, "from users"):
		return queryTestUsers(request), nil
	case strings.Contains(normalized, "from categories"):
		return cloneStringRows(testCategoryRows), nil
	case strings.Contains(normalized, "from customers"):
		return queryTestCustomers(request), nil
	case strings.Contains(normalized, "from orders"):
		return queryTestOrders(request), nil
	case strings.Contains(normalized, "from order_items"):
		return queryTestOrderItems(request), nil
	case strings.Contains(normalized, "from products"):
		return queryTestProducts(request), nil
	case normalized == "select 1 as value;":
		return []map[string]string{{"value": "1"}}, nil
	case normalized == "select 2 as value;":
		return []map[string]string{{"value": "2"}}, nil
	case normalized == "select 7 as value;":
		return []map[string]string{{"value": "7"}}, nil
	case normalized == "select 9 as value;":
		return []map[string]string{{"value": "9"}}, nil
	default:
		return nil, fmt.Errorf("unsupported test query: %s", request.SQL)
	}
}

func queryTestUsers(request SQLRequest) []map[string]string {
	rows := cloneStringRows(testUserRows)
	tenant := firstNamedOrPositionalString(request.Args, "tenantCode", 0)
	if tenant != "" {
		filtered := make([]map[string]string, 0, len(rows))
		for _, row := range rows {
			if strings.EqualFold(row["tenant"], tenant) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	keyword := strings.Trim(firstNamedOrPositionalString(request.Args, "keywordLike", 1), "%")
	if keyword == "" && strings.Contains(normalizeTestSQL(request.SQL), " like ") {
		keyword = firstPositionalString(request.Args, 1)
	}
	if keyword != "" {
		keyword = strings.ToLower(keyword)
		filtered := make([]map[string]string, 0, len(rows))
		for _, row := range rows {
			if strings.Contains(strings.ToLower(row["name"]), keyword) || strings.Contains(strings.ToLower(row["email"]), keyword) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	return rows
}

func queryTestCustomers(request SQLRequest) []map[string]string {
	normalized := normalizeTestSQL(request.SQL)
	email := strings.ToLower(firstNamedOrPositionalString(request.Args, "customerEmailKey", 0))
	rows := make([]map[string]string, 0, 1)
	for _, row := range testCustomerRows {
		if strings.ToLower(row["email"]) == email {
			selected := cloneStringRow(row)
			if !strings.Contains(normalized, "level") {
				delete(selected, "level")
			}
			rows = append(rows, selected)
		}
	}
	return rows
}

func queryTestOrders(request SQLRequest) []map[string]string {
	customerID := firstPositionalInt(request.Args, 0)
	status := strings.ToLower(firstNamedOrPositionalString(request.Args, "orderStatus", 1))
	rows := make([]map[string]string, 0)
	for _, row := range testOrderRows {
		if parseTestInt(row["customer_id"]) != customerID {
			continue
		}
		if status != "" && strings.ToLower(row["status"]) != status {
			continue
		}
		rows = append(rows, cloneStringRow(row))
	}
	return rows
}

func queryTestOrderItems(request SQLRequest) []map[string]string {
	if strings.Contains(normalizeTestSQL(request.SQL), "1 = 0") {
		return []map[string]string{}
	}

	idSet := make(map[int64]struct{})
	for _, value := range positionalValues(request.Args) {
		idSet[toTestInt(value)] = struct{}{}
	}

	rows := make([]map[string]string, 0)
	for _, row := range testOrderItemRows {
		if _, ok := idSet[parseTestInt(row["order_id"])]; ok {
			rows = append(rows, cloneStringRow(row))
		}
	}
	return rows
}

func queryTestProducts(request SQLRequest) []map[string]string {
	if strings.Contains(normalizeTestSQL(request.SQL), "1 = 0") {
		return []map[string]string{}
	}

	idSet := make(map[int64]struct{})
	for _, value := range positionalValues(request.Args) {
		idSet[toTestInt(value)] = struct{}{}
	}

	rows := make([]map[string]string, 0)
	for _, row := range testProductRows {
		if _, ok := idSet[parseTestInt(row["id"])]; ok {
			rows = append(rows, cloneStringRow(row))
		}
	}
	return rows
}

func normalizeTestSQL(sqlText string) string {
	return strings.ToLower(strings.Join(strings.Fields(sqlText), " "))
}

func cloneStringRows(rows []map[string]string) []map[string]string {
	cloned := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, cloneStringRow(row))
	}
	return cloned
}

func cloneStringRow(row map[string]string) map[string]string {
	cloned := make(map[string]string, len(row))
	for key, value := range row {
		cloned[key] = value
	}
	return cloned
}

func firstNamedOrPositionalString(args []any, name string, pos int) string {
	if value, ok := namedArgValue(args, name); ok {
		return fmt.Sprint(value)
	}
	return firstPositionalString(args, pos)
}

func firstPositionalString(args []any, pos int) string {
	values := positionalValues(args)
	if pos < 0 || pos >= len(values) {
		return ""
	}
	return fmt.Sprint(values[pos])
}

func firstPositionalInt(args []any, pos int) int64 {
	values := positionalValues(args)
	if pos < 0 || pos >= len(values) {
		return 0
	}
	return toTestInt(values[pos])
}

func namedArgValue(args []any, name string) (any, bool) {
	for _, arg := range args {
		named, ok := arg.(sql.NamedArg)
		if ok && strings.EqualFold(named.Name, name) {
			return named.Value, true
		}
	}
	return nil, false
}

func positionalValues(args []any) []any {
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

func toTestInt(value any) int64 {
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
		return parseTestInt(typed)
	default:
		return parseTestInt(fmt.Sprint(value))
	}
}

func parseTestInt(value string) int64 {
	number, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return number
}

func newExecTestDB(t *testing.T) *sql.DB {
	t.Helper()

	execTestDriverOnce.Do(func() {
		sql.Register("go-future-exec-test", execTestDriver{})
	})

	dsn := fmt.Sprintf("exec-%d", execTestDBSeq.Add(1))
	execTestDBStates.Store(dsn, &execTestState{
		users: map[string]string{
			"carol@labs.ai": "inactive",
		},
	})

	db, err := sql.Open("go-future-exec-test", dsn)
	if err != nil {
		t.Fatalf("open exec test db: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		execTestDBStates.Delete(dsn)
	})

	return db
}

type execTestDriver struct{}

func (execTestDriver) Open(name string) (driver.Conn, error) {
	value, ok := execTestDBStates.Load(name)
	if !ok {
		return nil, fmt.Errorf("unknown exec test db %q", name)
	}
	return &execTestConn{state: value.(*execTestState)}, nil
}

type execTestState struct {
	mu    sync.Mutex
	users map[string]string
}

type execTestConn struct {
	state *execTestState
}

func (c *execTestConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not supported")
}

func (c *execTestConn) Close() error { return nil }

func (c *execTestConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions not supported")
}

func (c *execTestConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_ = ctx
	if strings.Contains(normalizeTestSQL(query), "update users") {
		tenant := strings.ToUpper(namedDriverString(args, "tenantCode"))
		email := strings.ToLower(namedDriverString(args, "emailKey"))
		status := namedDriverString(args, "statusValue")

		c.state.mu.Lock()
		defer c.state.mu.Unlock()

		if tenant == "LABS" && email == "carol@labs.ai" {
			c.state.users[email] = status
			return execTestResult{rowsAffected: 1}, nil
		}

		return execTestResult{rowsAffected: 0}, nil
	}

	return nil, fmt.Errorf("unsupported exec query: %s", query)
}

func (c *execTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	_ = ctx
	_ = args
	if strings.Contains(normalizeTestSQL(query), "select status from users") {
		c.state.mu.Lock()
		status := c.state.users["carol@labs.ai"]
		c.state.mu.Unlock()

		return &execTestRows{
			columns: []string{"status"},
			values:  [][]driver.Value{{status}},
		}, nil
	}

	return nil, fmt.Errorf("unsupported query: %s", query)
}

type execTestResult struct {
	rowsAffected int64
}

func (r execTestResult) LastInsertId() (int64, error) { return 0, nil }

func (r execTestResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type execTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *execTestRows) Columns() []string {
	return append([]string(nil), r.columns...)
}

func (r *execTestRows) Close() error { return nil }

func (r *execTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func namedDriverString(args []driver.NamedValue, name string) string {
	for _, arg := range args {
		if strings.EqualFold(arg.Name, name) {
			return fmt.Sprint(arg.Value)
		}
	}
	return ""
}

var testUserRows = []map[string]string{
	{"id": "1", "tenant": "ACME", "name": "Alice Zhang", "email": "alice@acme.ai", "status": "active"},
	{"id": "2", "tenant": "ACME", "name": "Bob Li", "email": "bob@acme.ai", "status": "active"},
	{"id": "3", "tenant": "LABS", "name": "Carol Wu", "email": "carol@labs.ai", "status": "inactive"},
	{"id": "4", "tenant": "LABS", "name": "David Chen", "email": "david@labs.ai", "status": "active"},
}

var testCategoryRows = []map[string]string{
	{"id": "1", "parent_id": "0", "name": "AI"},
	{"id": "2", "parent_id": "1", "name": "Agents"},
	{"id": "3", "parent_id": "2", "name": "Planning"},
	{"id": "4", "parent_id": "2", "name": "Execution"},
	{"id": "5", "parent_id": "1", "name": "Workflows"},
}

var testCustomerRows = []map[string]string{
	{"id": "1", "name": "Alice Future", "email": "alice.future@demo.ai", "level": "gold"},
	{"id": "2", "name": "Bob Labs", "email": "bob.labs@demo.ai", "level": "silver"},
}

var testProductRows = []map[string]string{
	{"id": "1", "name": "Copilot Seat", "price": "99"},
	{"id": "2", "name": "Workflow Engine", "price": "249"},
	{"id": "3", "name": "Knowledge Pack", "price": "49"},
}

var testOrderRows = []map[string]string{
	{"id": "1", "customer_id": "1", "order_no": "SO-1001", "status": "paid", "created_at": "2026-04-01T10:00:00Z"},
	{"id": "2", "customer_id": "1", "order_no": "SO-1002", "status": "pending", "created_at": "2026-04-03T14:30:00Z"},
	{"id": "3", "customer_id": "2", "order_no": "SO-1003", "status": "paid", "created_at": "2026-04-04T09:15:00Z"},
}

var testOrderItemRows = []map[string]string{
	{"id": "1", "order_id": "1", "product_id": "1", "quantity": "2", "unit_price": "99", "amount": "198"},
	{"id": "2", "order_id": "1", "product_id": "3", "quantity": "1", "unit_price": "49", "amount": "49"},
	{"id": "3", "order_id": "2", "product_id": "2", "quantity": "1", "unit_price": "249", "amount": "249"},
	{"id": "4", "order_id": "3", "product_id": "1", "quantity": "1", "unit_price": "99", "amount": "99"},
	{"id": "5", "order_id": "3", "product_id": "2", "quantity": "1", "unit_price": "249", "amount": "249"},
}
