package workflow

import (
	"database/sql"

	engine "go-ai-future/internal/workflow"
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
	internalWorkflows := make([]*engine.Workflow, len(workflows))
	copy(internalWorkflows, workflows)
	return engine.NewCatalog(internalWorkflows...)
}
