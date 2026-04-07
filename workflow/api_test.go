package workflow_test

import (
	"context"
	"database/sql"
	"testing"

	"go-ai-future/internal/data"
	"go-ai-future/workflow"

	_ "modernc.org/sqlite"
)

func TestPublicWorkflowPackage(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if err := data.EnsureDemoData(context.Background(), db); err != nil {
		t.Fatalf("bootstrap demo data: %v", err)
	}

	wf, err := workflow.ParseString(`<workflow name="public-lib" title="Public Library">
  <input name="tenant" required="true" />
  <input name="keyword" default="" />
  <var name="tenantCode" from="tenant" op="trim,upper" />
  <var name="keyword" from="keyword" default="" op="trim" />
  <var name="keywordLike" template="%{{keyword}}%" />
  <sql name="users" mode="query"><![CDATA[
SELECT id, tenant, name, email, status
FROM users
WHERE tenant = :tenantCode
  AND (:keyword = '' OR name LIKE :keywordLike OR email LIKE :keywordLike)
ORDER BY id;
  ]]></sql>
</workflow>`, "")
	if err != nil {
		t.Fatalf("ParseString() error = %v", err)
	}

	catalog, err := workflow.NewCatalog(wf)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	loaded, ok := catalog.Get("public-lib")
	if !ok || loaded == nil {
		t.Fatalf("expected public-lib to be present in catalog")
	}

	executor := workflow.NewExecutor(map[string]*sql.DB{
		"default": db,
	})

	result, err := executor.Run(context.Background(), wf, map[string]string{
		"tenant":  "acme",
		"keyword": "Alice",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Query == nil || len(result.Query.Rows) != 1 {
		t.Fatalf("expected one query row, got %#v", result.Query)
	}

	if result.Query.Rows[0]["name"] != "Alice Zhang" {
		t.Fatalf("expected Alice Zhang, got %#v", result.Query.Rows[0]["name"])
	}
}
