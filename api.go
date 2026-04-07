package future

import (
	"context"
	"database/sql"

	"github.com/llyb120/go-future/internal/data"
	"github.com/llyb120/go-future/internal/web"
	engine "github.com/llyb120/go-future/internal/workflow"
)

type Catalog = engine.Catalog
type Workflow = engine.Workflow
type Input = engine.Input
type Step = engine.Step
type Executor = engine.Executor
type Execution = engine.Execution
type ResolvedParam = engine.ResolvedParam
type QueryResult = engine.QueryResult
type ExecResult = engine.ExecResult
type Server = web.Server
type PageData = web.PageData

func NewExecutor(dbs map[string]*sql.DB) *Executor {
	return engine.NewExecutor(dbs)
}

func LoadDir(dir string) (*Catalog, error) {
	return engine.LoadDir(dir)
}

func LoadFile(path string) (*Workflow, error) {
	return engine.LoadFile(path)
}

func Parse(content []byte, sourcePath string) (*Workflow, error) {
	return engine.Parse(content, sourcePath)
}

func ParseString(content string, sourcePath string) (*Workflow, error) {
	return engine.ParseString(content, sourcePath)
}

func NewCatalog(workflows ...*Workflow) (*Catalog, error) {
	return engine.NewCatalog(workflows...)
}

func NewServer(catalog *Catalog, executor *Executor) (*Server, error) {
	return web.NewServer(catalog, executor)
}

func EnsureDemoData(ctx context.Context, db *sql.DB) error {
	return data.EnsureDemoData(ctx, db)
}
