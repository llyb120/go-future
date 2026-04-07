package web

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/llyb120/go-future/internal/data"
	"github.com/llyb120/go-future/internal/workflow"

	_ "modernc.org/sqlite"
)

func TestServerRendersWorkflowAndRunsIt(t *testing.T) {
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

	dir := t.TempDir()
	xmlPath := filepath.Join(dir, "user-search.xml")
	if err := os.WriteFile(xmlPath, []byte(sampleWorkflowXML), 0o644); err != nil {
		t.Fatalf("write workflow xml: %v", err)
	}

	catalog, err := workflow.LoadDir(dir)
	if err != nil {
		t.Fatalf("load workflow dir: %v", err)
	}

	server, err := NewServer(catalog, workflow.NewExecutor(map[string]*sql.DB{"default": db}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	mux := http.NewServeMux()
	server.Register(mux)

	indexResponse := httptest.NewRecorder()
	mux.ServeHTTP(indexResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if indexResponse.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", indexResponse.Code)
	}

	if !strings.Contains(indexResponse.Body.String(), "租户用户查询") {
		t.Fatalf("GET / body does not include workflow title")
	}

	form := url.Values{
		"workflow": {"user-search"},
		"tenant":   {"acme"},
		"keyword":  {"Alice"},
		"limit":    {"2"},
	}

	runRequest := httptest.NewRequest(http.MethodPost, "/run", strings.NewReader(form.Encode()))
	runRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	runResponse := httptest.NewRecorder()
	mux.ServeHTTP(runResponse, runRequest)

	if runResponse.Code != http.StatusOK {
		t.Fatalf("POST /run status = %d", runResponse.Code)
	}

	body, err := io.ReadAll(runResponse.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if !strings.Contains(string(body), "Alice Zhang") {
		t.Fatalf("POST /run body does not include query result")
	}
}

const sampleWorkflowXML = `<workflow name="user-search" title="租户用户查询" description="先整理参数，再执行 SQL">
  <input name="tenant" label="租户编码" required="true" placeholder="例如 acme" />
  <input name="keyword" label="关键字" default="" placeholder="姓名或邮箱，可留空" />
  <input name="limit" label="返回条数" default="10" type="number" />
  <var name="tenantCode" from="tenant" op="trim,upper" />
  <var name="keyword" from="keyword" default="" op="trim" />
  <var name="keywordLike" template="%{{keyword}}%" />
  <var name="limitNum" from="limit" default="10" op="trim,int" />
  <sql mode="query" datasource="default"><![CDATA[
SELECT id, tenant, name, email, status
FROM users
WHERE tenant = :tenantCode
  AND (:keyword = '' OR name LIKE :keywordLike OR email LIKE :keywordLike)
ORDER BY id
LIMIT :limitNum;
  ]]></sql>
</workflow>`
