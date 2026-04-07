package future_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	future "github.com/llyb120/go-future"

	_ "modernc.org/sqlite"
)

func TestRootPackageRunsBundledWorkflowAndBuildsServer(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if err := future.EnsureDemoData(context.Background(), db); err != nil {
		t.Fatalf("bootstrap demo data: %v", err)
	}

	catalog, err := future.LoadDir("workflows")
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	selected, ok := catalog.Get("customer-orders-structured")
	if !ok || selected == nil {
		t.Fatalf("expected bundled workflow to be present")
	}

	executor := future.NewExecutor(map[string]*sql.DB{
		"default": db,
	})

	result, err := executor.Run(context.Background(), selected, map[string]string{
		"customerEmail": "alice.future@demo.ai",
		"orderStatus":   "paid",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	customerID, ok := resolvedValue(result.Resolved, "customerId").(int64)
	if !ok || customerID != 1 {
		t.Fatalf("expected customerId=1, got %#v", resolvedValue(result.Resolved, "customerId"))
	}

	server, err := future.NewServer(catalog, executor)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	mux := http.NewServeMux()
	server.Register(mux)

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", response.Code)
	}

	if !strings.Contains(response.Body.String(), "客户订单结构视图") {
		t.Fatalf("GET / body does not include bundled workflow title")
	}

	external, ok := catalog.Get("customer-orders-external")
	if !ok || external == nil {
		t.Fatalf("expected bundled external workflow to be present")
	}

	externalResult, err := executor.Run(context.Background(), external, map[string]string{
		"customerEmail": "alice.future@demo.ai",
		"orderStatus":   "paid",
	})
	if err != nil {
		t.Fatalf("external Run() error = %v", err)
	}

	if _, ok := resolvedValue(externalResult.Resolved, "customerOrderStats").(map[string]any); !ok {
		t.Fatalf("expected customerOrderStats from preloaded resources, got %#v", resolvedValue(externalResult.Resolved, "customerOrderStats"))
	}
}

func resolvedValue(params []future.ResolvedParam, name string) any {
	for _, param := range params {
		if param.Name == name {
			return param.Value
		}
	}
	return nil
}
