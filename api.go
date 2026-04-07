package future

import (
	"database/sql"
	"io/fs"

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
type SQLRequest = engine.SQLRequest
type SQLExecutor = engine.SQLExecutor
type SQLExecutorFunc = engine.SQLExecutorFunc

func NewExecutor(dbs map[string]*sql.DB) *Executor {
	return engine.NewExecutor(dbs)
}

func NewExecutorWithSQLExecutors(dbs map[string]*sql.DB, executors map[string]SQLExecutor) *Executor {
	return engine.NewExecutorWithSQLExecutors(dbs, executors)
}

func LoadDir(dir string) (*Catalog, error) {
	return engine.LoadDir(dir)
}

func LoadDirFS(fsys fs.FS, dir string) (*Catalog, error) {
	return engine.LoadDirFS(fsys, dir)
}

func LoadFile(path string) (*Workflow, error) {
	return engine.LoadFile(path)
}

func LoadFileFS(fsys fs.FS, path string) (*Workflow, error) {
	return engine.LoadFileFS(fsys, path)
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
