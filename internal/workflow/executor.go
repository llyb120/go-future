package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"go-ai-future/internal/jsruntime"

	"github.com/llyb120/gosql"
)

var (
	templateVarPattern = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)
	sqlParamPattern    = regexp.MustCompile(`:([A-Za-z_][A-Za-z0-9_]*)`)
	jsExportPattern    = regexp.MustCompile(`(?m)^\s*export\s+(async\s+)?function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(`)
)

type Executor struct {
	dbs map[string]*sql.DB
}

type Execution struct {
	WorkflowName  string
	WorkflowTitle string
	SQL           string
	SQLMode       string
	Resolved      []ResolvedParam
	Query         *QueryResult
	Exec          *ExecResult
}

type ResolvedParam struct {
	Name  string
	Value any
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]any
}

type ExecResult struct {
	RowsAffected int64
	LastInsertID int64
}

type selectorToken struct {
	kind     string
	key      string
	index    int
	op       string
	value    string
	relation string
}

func NewExecutor(dbs map[string]*sql.DB) *Executor {
	copied := make(map[string]*sql.DB, len(dbs))
	for name, db := range dbs {
		copied[name] = db
	}

	return &Executor{dbs: copied}
}

func (e *Executor) Run(ctx context.Context, wf *Workflow, incoming map[string]string) (*Execution, error) {
	if wf == nil {
		return nil, fmt.Errorf("workflow is required")
	}

	if err := wf.Validate(); err != nil {
		return nil, err
	}

	vars := make(map[string]any, len(wf.Inputs)+len(wf.Steps))
	for _, input := range wf.Inputs {
		rawValue, exists := incoming[input.Name]
		if !exists {
			rawValue = input.InitialValue()
		}

		value, err := parseInputValue(input, rawValue)
		if err != nil {
			return nil, err
		}

		vars[input.Name] = value
	}

	execution := &Execution{
		WorkflowName:  wf.Name,
		WorkflowTitle: wf.Title,
	}

	for _, step := range wf.Steps {
		if err := e.executeStep(ctx, wf, step, vars, execution); err != nil {
			return nil, err
		}
	}

	execution.Resolved = sortedParams(vars)
	return execution, nil
}

func parseInputValue(input Input, raw string) (any, error) {
	if input.Required && strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("input %q is required", input.Name)
	}

	switch strings.ToLower(strings.TrimSpace(input.Type)) {
	case "json":
		if strings.TrimSpace(raw) == "" {
			return nil, nil
		}

		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, fmt.Errorf("input %q parse json: %w", input.Name, err)
		}

		return value, nil
	default:
		return raw, nil
	}
}

func (e *Executor) executeStep(ctx context.Context, wf *Workflow, step Step, vars map[string]any, execution *Execution) error {
	switch step.Kind {
	case "var":
		value, err := e.evaluateVar(wf, step, vars)
		if err != nil {
			return err
		}
		vars[step.Name] = value
	case "pick":
		value, err := e.evaluatePick(step, vars, nil)
		if err != nil {
			return err
		}
		vars[step.Name] = value
	case "transform":
		value, err := e.evaluateTransform(wf, step, vars, nil)
		if err != nil {
			return err
		}
		vars[step.Name] = value
	case "set":
		value, err := e.evaluateLegacySet(step, vars)
		if err != nil {
			return err
		}
		vars[step.Name] = value
	case "sql":
		return e.executeSQL(ctx, wf, step, vars, execution)
	default:
		return fmt.Errorf("unsupported step <%s>", step.Kind)
	}

	return nil
}

func (e *Executor) evaluateVar(wf *Workflow, step Step, vars map[string]any) (any, error) {
	if len(step.Children) == 0 {
		return e.resolveValue(step, vars, nil, false)
	}

	var current any
	for _, child := range step.Children {
		var (
			next any
			err  error
		)

		switch child.Kind {
		case "pick":
			next, err = e.evaluatePick(child, vars, current)
		case "transform":
			next, err = e.evaluateTransform(wf, child, vars, current)
		case "set":
			next, err = e.evaluateLegacySet(child, vars)
		default:
			return nil, fmt.Errorf("<var name=%q> does not support nested <%s>", step.Name, child.Kind)
		}

		if err != nil {
			return nil, err
		}

		current = next
	}

	if isEmpty(current) && strings.TrimSpace(step.Default) != "" {
		value, err := renderTemplate(step.Default, vars, current)
		if err != nil {
			return nil, fmt.Errorf("<var name=%q> default: %w", step.Name, err)
		}
		current = value
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(current, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<var name=%q> apply operations: %w", step.Name, err)
		}
		current = value
	}

	return current, nil
}

func (e *Executor) evaluatePick(step Step, vars map[string]any, current any) (any, error) {
	sourceRef := strings.TrimSpace(step.From)
	if sourceRef == "" {
		if current != nil {
			sourceRef = "current"
		} else {
			sourceRef = "$"
		}
	}

	source, found, err := resolveDynamicReference(sourceRef, vars, current)
	if err != nil {
		return nil, fmt.Errorf("<pick> resolve source: %w", err)
	}
	if !found {
		if step.Optional {
			return nil, nil
		}
		return nil, fmt.Errorf("<pick> source %q not found", sourceRef)
	}

	selectorText := step.Path
	if strings.Contains(selectorText, "{{") {
		selectorText, err = renderTemplate(selectorText, vars, current)
		if err != nil {
			return nil, fmt.Errorf("<pick> render selector: %w", err)
		}
	}

	value, found, err := selectValue(source, selectorText)
	if err != nil {
		return nil, fmt.Errorf("<pick> apply selector %q: %w", selectorText, err)
	}
	if !found {
		if step.Optional {
			value = nil
		} else {
			return nil, fmt.Errorf("<pick> selector %q not found", selectorText)
		}
	}

	if isEmpty(value) && strings.TrimSpace(step.Default) != "" {
		value, err = renderTemplate(step.Default, vars, current)
		if err != nil {
			return nil, fmt.Errorf("<pick> default: %w", err)
		}
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err = applyOperations(value, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<pick> apply operations: %w", err)
		}
	}

	return value, nil
}

func (e *Executor) evaluateTransform(wf *Workflow, step Step, vars map[string]any, current any) (any, error) {
	switch step.TransformMode() {
	case "object":
		return e.evaluateObjectTransform(step, vars, current)
	case "map":
		return e.evaluateMapTransform(step, vars, current)
	case "group":
		return e.evaluateGroupTransform(step, vars, current)
	case "index":
		return e.evaluateIndexTransform(step, vars, current)
	case "js":
		return e.evaluateJSTransform(wf, step, vars, current)
	case "tree":
		return e.evaluateTreeTransform(step, vars, current)
	default:
		value, err := e.resolveValue(step, vars, current, true)
		if err != nil {
			return nil, err
		}

		return value, nil
	}
}

func (e *Executor) evaluateLegacySet(step Step, vars map[string]any) (any, error) {
	value, err := e.resolveValue(step, vars, nil, false)
	if err != nil {
		return nil, fmt.Errorf("<set name=%q>: %w", step.Name, err)
	}

	return value, nil
}

func (e *Executor) evaluateObjectTransform(step Step, vars map[string]any, current any) (any, error) {
	source := current
	if strings.TrimSpace(step.From) != "" {
		value, found, err := resolveDynamicReference(step.From, vars, current)
		if err != nil {
			return nil, err
		}
		if !found {
			if step.Optional {
				source = nil
			} else {
				return nil, fmt.Errorf("reference %q not found", step.From)
			}
		} else {
			source = value
		}
	}

	result := make(map[string]any, len(step.Children))
	for _, field := range step.Children {
		value, err := e.resolveValue(field, vars, source, true)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> field %q: %w", step.Name, field.TargetPath(), err)
		}

		if value == nil && field.Optional && strings.TrimSpace(field.Default) == "" {
			continue
		}

		if err := setObjectPath(result, field.TargetPath(), value); err != nil {
			return nil, fmt.Errorf("<transform name=%q> field %q: %w", step.Name, field.TargetPath(), err)
		}
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(result, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> apply operations: %w", step.Name, err)
		}
		return value, nil
	}

	return result, nil
}

func (e *Executor) evaluateMapTransform(step Step, vars map[string]any, current any) (any, error) {
	items, err := e.resolveTransformItems(step, vars, current)
	if err != nil {
		return nil, err
	}

	result := make([]any, 0, len(items))
	for _, item := range items {
		if len(step.Children) == 0 {
			result = append(result, item)
			continue
		}

		mapped, err := e.evaluateObjectTransform(Step{
			Name:     step.Name,
			Children: step.Children,
		}, vars, item)
		if err != nil {
			return nil, err
		}

		result = append(result, mapped)
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(result, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> apply operations: %w", step.Name, err)
		}
		return value, nil
	}

	return result, nil
}

func (e *Executor) evaluateGroupTransform(step Step, vars map[string]any, current any) (any, error) {
	items, err := e.resolveTransformItems(step, vars, current)
	if err != nil {
		return nil, err
	}

	grouped := make(map[string]any)
	for _, item := range items {
		groupKey, found, err := selectValue(item, step.By)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q mode=group> resolve by %q: %w", step.Name, step.By, err)
		}
		if !found {
			return nil, fmt.Errorf("<transform name=%q mode=group> key %q not found", step.Name, step.By)
		}

		groupValue := item
		if len(step.Children) > 0 {
			groupValue, err = e.evaluateObjectTransform(Step{
				Name:     step.Name,
				Children: step.Children,
			}, vars, item)
			if err != nil {
				return nil, err
			}
		}

		key := normalizeKey(groupKey)
		existing, _ := grouped[key].([]any)
		grouped[key] = append(existing, groupValue)
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(grouped, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> apply operations: %w", step.Name, err)
		}
		return value, nil
	}

	return grouped, nil
}

func (e *Executor) evaluateIndexTransform(step Step, vars map[string]any, current any) (any, error) {
	items, err := e.resolveTransformItems(step, vars, current)
	if err != nil {
		return nil, err
	}

	indexed := make(map[string]any, len(items))
	for _, item := range items {
		indexKey, found, err := selectValue(item, step.By)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q mode=index> resolve by %q: %w", step.Name, step.By, err)
		}
		if !found {
			return nil, fmt.Errorf("<transform name=%q mode=index> key %q not found", step.Name, step.By)
		}

		indexValue := item
		if len(step.Children) > 0 {
			indexValue, err = e.evaluateObjectTransform(Step{
				Name:     step.Name,
				Children: step.Children,
			}, vars, item)
			if err != nil {
				return nil, err
			}
		}

		indexed[normalizeKey(indexKey)] = indexValue
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(indexed, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> apply operations: %w", step.Name, err)
		}
		return value, nil
	}

	return indexed, nil
}

func (e *Executor) resolveTransformItems(step Step, vars map[string]any, current any) ([]any, error) {
	source, err := e.resolveValue(Step{
		From:     step.From,
		Value:    step.Value,
		Template: step.Template,
		Default:  step.Default,
		Optional: step.Optional,
	}, vars, current, true)
	if err != nil {
		return nil, err
	}

	items, ok := toSlice(source)
	if !ok {
		return nil, fmt.Errorf("<transform name=%q mode=%s> expects a slice, got %T", step.Name, step.TransformMode(), source)
	}

	return items, nil
}

func (e *Executor) evaluateTreeTransform(step Step, vars map[string]any, current any) (any, error) {
	source, err := e.resolveValue(Step{
		From:     step.From,
		Value:    step.Value,
		Template: step.Template,
		Default:  step.Default,
		Optional: step.Optional,
	}, vars, current, true)
	if err != nil {
		return nil, err
	}

	items, ok := toSlice(source)
	if !ok {
		return nil, fmt.Errorf("<transform name=%q mode=tree> expects a slice, got %T", step.Name, source)
	}

	rootValue := step.Root
	if strings.Contains(rootValue, "{{") {
		rendered, err := renderTemplate(rootValue, vars, current)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q mode=tree> root: %w", step.Name, err)
		}
		rootValue = rendered
	}

	childrenKey := step.ChildrenKey
	if strings.TrimSpace(childrenKey) == "" {
		childrenKey = "children"
	}

	tree, err := buildTree(items, step.IDField, step.ParentField, childrenKey, rootValue)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(tree, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> apply operations: %w", step.Name, err)
		}
		return value, nil
	}

	return tree, nil
}

func (e *Executor) evaluateJSTransform(wf *Workflow, step Step, vars map[string]any, current any) (any, error) {
	source, err := e.resolveValue(Step{
		From:     step.From,
		Value:    step.Value,
		Template: step.Template,
		Default:  step.Default,
		Optional: step.Optional,
	}, vars, current, true)
	if err != nil {
		return nil, err
	}

	rt, err := jsruntime.New()
	if err != nil {
		return nil, fmt.Errorf("create javascript runtime: %w", err)
	}
	defer rt.Close()

	if err := rt.Register("pick", func(args []any) (any, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("host.pick expects 2 arguments: value and selector")
		}

		selector, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("host.pick selector must be a string")
		}

		value, found, err := selectValue(args[0], selector)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		return value, nil
	}); err != nil {
		return nil, fmt.Errorf("register javascript host pick: %w", err)
	}

	filename := step.Name + ".js"
	if strings.TrimSpace(step.Name) == "" {
		filename = "workflow_transform.js"
	}

	scriptBody, _, err := resolveStepBody(wf, step, "js")
	if err != nil {
		return nil, err
	}

	scriptSource, entryName, err := prepareJSTransformScript(scriptBody, step.Entry)
	if err != nil {
		return nil, err
	}
	if err := rt.LoadScript(filename, scriptSource); err != nil {
		return nil, fmt.Errorf("load javascript transform: %w", err)
	}

	result, err := rt.Call(entryName, map[string]any{
		"input":   source,
		"vars":    vars,
		"current": current,
	})
	if err != nil {
		return nil, fmt.Errorf("execute javascript transform: %w", err)
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err := applyOperations(result, step.Op)
		if err != nil {
			return nil, fmt.Errorf("<transform name=%q> apply operations: %w", step.Name, err)
		}
		return value, nil
	}

	return result, nil
}

func prepareJSTransformScript(source string, entry string) (string, string, error) {
	entry = strings.TrimSpace(entry)

	if strings.Contains(source, "host.export(") {
		if entry == "" {
			entry = "run"
		}
		return source, entry, nil
	}

	if strings.Contains(source, "export ") {
		return buildExportedJSTransformScript(source, entry)
	}

	if entry == "" {
		entry = "run"
	}

	return `host.export("` + entry + `", async (payload) => {
  const input = payload?.input ?? null;
  const vars = payload?.vars ?? {};
  const current = payload?.current ?? null;
  const pick = async (value, selector) => await host.pick(value, selector);
  const keys = async (value) => await pick(value, ":keys");
  const asArray = (value) => Array.isArray(value) ? value : (value == null ? [] : [value]);

` + source + `
});`, entry, nil
}

func buildExportedJSTransformScript(source string, entry string) (string, string, error) {
	matches := jsExportPattern.FindAllStringSubmatch(source, -1)
	if len(matches) == 0 {
		return "", "", fmt.Errorf("javascript source contains export syntax but no supported exported function declaration")
	}

	exportedNames := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		name := strings.TrimSpace(match[2])
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		exportedNames = append(exportedNames, name)
	}

	if entry == "" {
		if len(exportedNames) != 1 {
			return "", "", fmt.Errorf("javascript source exports multiple functions; please specify entry")
		}
		entry = exportedNames[0]
	} else if _, exists := seen[entry]; !exists {
		return "", "", fmt.Errorf("javascript source does not export function %q", entry)
	}

	transformedSource := jsExportPattern.ReplaceAllString(source, "${1}function ${2}(")

	var exportBuilder strings.Builder
	exportBuilder.WriteString(`const workflowPick = async (value, selector) => await host.pick(value, selector);
const workflowKeys = async (value) => await workflowPick(value, ":keys");
const workflowAsArray = (value) => Array.isArray(value) ? value : (value == null ? [] : [value]);

`)
	exportBuilder.WriteString(transformedSource)
	exportBuilder.WriteString("\n\n")

	for _, name := range exportedNames {
		exportBuilder.WriteString(`host.export("`)
		exportBuilder.WriteString(name)
		exportBuilder.WriteString(`", async (payload) => await `)
		exportBuilder.WriteString(name)
		exportBuilder.WriteString(`({
  input: payload?.input ?? null,
  vars: payload?.vars ?? {},
  current: payload?.current ?? null,
  pick: workflowPick,
  keys: workflowKeys,
  asArray: workflowAsArray,
}));` + "\n")
	}

	return exportBuilder.String(), entry, nil
}

func (e *Executor) resolveValue(step Step, vars map[string]any, current any, allowCurrent bool) (any, error) {
	var (
		value any
		found bool
		err   error
	)

	switch {
	case strings.TrimSpace(step.Template) != "" || strings.TrimSpace(step.Text) != "":
		value, err = renderTemplate(step.Body(), vars, current)
		if err != nil {
			return nil, err
		}
	case strings.TrimSpace(step.Value) != "":
		value = step.Value
	case strings.TrimSpace(step.From) != "":
		value, found, err = resolveDynamicReference(step.From, vars, current)
		if err != nil {
			return nil, err
		}
		if !found {
			if step.Optional {
				value = nil
			} else {
				return nil, fmt.Errorf("reference %q not found", step.From)
			}
		}
	case allowCurrent:
		value = current
	default:
		value = nil
	}

	if isEmpty(value) && strings.TrimSpace(step.Default) != "" {
		value, err = renderTemplate(step.Default, vars, current)
		if err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(step.Op) != "" {
		value, err = applyOperations(value, step.Op)
		if err != nil {
			return nil, err
		}
	}

	return value, nil
}

func (e *Executor) executeSQL(ctx context.Context, wf *Workflow, step Step, vars map[string]any, execution *Execution) error {
	datasource := step.Datasource
	if datasource == "" {
		datasource = "default"
	}

	db, ok := e.dbs[datasource]
	if !ok {
		return fmt.Errorf("datasource %q is not configured", datasource)
	}

	var (
		sourceLanguage string
		sourceText     string
		sqlText        string
		args           []any
		err            error
	)

	sourceText, sourceLanguage, err = resolveStepBody(wf, step, "sql")
	if err != nil {
		return err
	}

	switch effectiveSQLEngine(step, sourceLanguage) {
	case "gosql":
		sqlText, args, err = renderGoSQL(sourceText, vars)
	default:
		sqlText = strings.TrimSpace(sourceText)
		args, err = plainSQLArgs(sqlText, vars)
	}
	if err != nil {
		return err
	}

	execution.SQL = sqlText
	execution.SQLMode = step.NormalizedMode()

	if step.NormalizedMode() == "exec" {
		result, err := db.ExecContext(ctx, sqlText, args...)
		if err != nil {
			return fmt.Errorf("execute sql: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read rows affected: %w", err)
		}

		lastInsertID, err := result.LastInsertId()
		if err != nil {
			lastInsertID = 0
		}

		execution.Exec = &ExecResult{
			RowsAffected: rowsAffected,
			LastInsertID: lastInsertID,
		}

		if strings.TrimSpace(step.Name) != "" {
			vars[step.Name] = map[string]any{
				"rowsAffected": rowsAffected,
				"lastInsertID": lastInsertID,
			}
		}

		return nil
	}

	rows, err := db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return fmt.Errorf("query sql: %w", err)
	}
	defer rows.Close()

	queryResult, err := scanRows(rows)
	if err != nil {
		return err
	}

	execution.Query = queryResult

	if strings.TrimSpace(step.Name) != "" {
		vars[step.Name] = queryResult.Rows
	}

	return nil
}

func renderGoSQL(sourceText string, vars map[string]any) (string, []any, error) {
	engine := gosql.New()

	markdown := "# workflow\n## main\n```sql\n" + strings.TrimSpace(sourceText) + "\n```\n"
	if err := engine.LoadMarkdown(markdown); err != nil {
		return "", nil, fmt.Errorf("load gosql template: %w", err)
	}

	query, err := engine.GetSql("workflow.main", vars)
	if err != nil {
		return "", nil, fmt.Errorf("render gosql template: %w", err)
	}

	return strings.TrimSpace(query.SQL), query.Params, nil
}

func renderTemplate(input string, vars map[string]any, current any) (string, error) {
	missing := ""

	output := templateVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		submatches := templateVarPattern.FindStringSubmatch(match)
		if len(submatches) != 2 {
			return match
		}

		reference := strings.TrimSpace(submatches[1])
		value, found, err := resolveReference(reference, vars, current)
		if err != nil {
			missing = err.Error()
			return ""
		}
		if !found {
			missing = reference
			return ""
		}
		if value == nil {
			return ""
		}

		return fmt.Sprint(value)
	})

	if missing != "" {
		return "", fmt.Errorf("unknown variable or path %q", missing)
	}

	return output, nil
}

func resolveDynamicReference(reference string, vars map[string]any, current any) (any, bool, error) {
	if strings.Contains(reference, "{{") {
		rendered, err := renderTemplate(reference, vars, current)
		if err != nil {
			return nil, false, err
		}
		reference = rendered
	}

	return resolveReference(reference, vars, current)
}

func resolveReference(reference string, vars map[string]any, current any) (any, bool, error) {
	reference = strings.TrimSpace(reference)
	switch {
	case reference == "", reference == "current", reference == ".":
		if reference == "" {
			return nil, false, nil
		}
		return current, true, nil
	case reference == "$":
		return vars, true, nil
	case strings.HasPrefix(reference, "current."):
		return selectValue(current, strings.TrimPrefix(reference, "current."))
	case strings.HasPrefix(reference, "current["):
		return selectValue(current, strings.TrimPrefix(reference, "current"))
	case strings.HasPrefix(reference, "."):
		return selectValue(current, strings.TrimPrefix(reference, "."))
	case strings.HasPrefix(reference, "$."):
		return selectValue(vars, strings.TrimPrefix(reference, "$."))
	case strings.HasPrefix(reference, "$["):
		return selectValue(vars, strings.TrimPrefix(reference, "$"))
	default:
		value, found, err := selectValue(vars, reference)
		if err != nil || found {
			return value, found, err
		}
		if current != nil {
			return selectValue(current, reference)
		}
		return nil, false, nil
	}
}

func selectValue(source any, selector string) (any, bool, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return source, true, nil
	}

	tokens, err := parseSelector(selector)
	if err != nil {
		return nil, false, err
	}

	return walkSelection(source, tokens)
}

func parseSelector(selector string) ([]selectorToken, error) {
	selector = strings.TrimSpace(selector)
	selector = strings.TrimPrefix(selector, "$.")
	if selector == "$" || selector == "" {
		return nil, nil
	}
	if strings.HasPrefix(selector, "$") {
		selector = strings.TrimPrefix(selector, "$")
	}

	tokens := make([]selectorToken, 0)
	pendingRelation := ""
	sawToken := false

	for index := 0; index < len(selector); {
		switch {
		case isSelectorSpace(selector[index]):
			for index < len(selector) && isSelectorSpace(selector[index]) {
				index++
			}
			if index < len(selector) && selector[index] == '>' {
				continue
			}
			if sawToken && pendingRelation == "" {
				pendingRelation = "descendant"
			}
		case selector[index] == '>':
			if !sawToken {
				return nil, fmt.Errorf("selector %q cannot start with >", selector)
			}
			if pendingRelation != "" {
				return nil, fmt.Errorf("selector %q has mixed separators at the same level", selector)
			}
			pendingRelation = "child"
			index++
			for index < len(selector) && isSelectorSpace(selector[index]) {
				index++
			}
		case selector[index] == '[':
			content, nextIndex, err := readSelectorGroup(selector, index, '[', ']')
			if err != nil {
				return nil, err
			}
			token, err := parseBracketSelector(content)
			if err != nil {
				return nil, err
			}
			token.relation = pendingRelation
			tokens = append(tokens, token)
			index = nextIndex
			pendingRelation = ""
			sawToken = true
		case selector[index] == ':':
			name, arg, nextIndex, err := parsePseudoSelector(selector, index)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, selectorToken{
				kind:     "pseudo",
				key:      name,
				value:    arg,
				relation: pendingRelation,
			})
			index = nextIndex
			pendingRelation = ""
			sawToken = true
		default:
			start := index
			for index < len(selector) && !strings.ContainsRune("[>: \t\r\n", rune(selector[index])) {
				index++
			}

			key := strings.TrimSpace(selector[start:index])
			if key != "" {
				tokens = append(tokens, selectorToken{
					kind:     "field",
					key:      key,
					relation: pendingRelation,
				})
				pendingRelation = ""
				sawToken = true
			}
		}
	}

	if pendingRelation != "" {
		return nil, fmt.Errorf("selector %q cannot end with a separator", selector)
	}

	return tokens, nil
}

func walkSelection(value any, tokens []selectorToken) (any, bool, error) {
	if len(tokens) == 0 {
		return value, true, nil
	}

	token := tokens[0]
	if token.relation == "descendant" {
		includeCurrent := token.kind == "field" && !isSelectorSequence(value)
		return walkDescendantSelection(value, token, tokens[1:], includeCurrent)
	}

	return walkTokenMatch(value, token, tokens[1:])
}

func applySelectorToken(value any, token selectorToken) (any, bool, error) {
	switch token.kind {
	case "field":
		return applyFieldSelector(value, token.key)
	case "index":
		items, ok := toSlice(value)
		if !ok || token.index < 0 || token.index >= len(items) {
			return nil, false, nil
		}
		return items[token.index], true, nil
	case "wildcard":
		items, ok := toSlice(value)
		if !ok {
			return nil, false, nil
		}
		return items, true, nil
	case "attr":
		return applyAttributeSelector(value, token.key, token.op, token.value)
	case "pseudo":
		return applyPseudoSelector(value, token.key, token.value)
	default:
		return nil, false, fmt.Errorf("unsupported selector token kind %q", token.kind)
	}
}

func walkTokenMatch(value any, token selectorToken, remaining []selectorToken) (any, bool, error) {
	if token.kind == "wildcard" {
		items, ok := toSlice(value)
		if !ok {
			return nil, false, nil
		}

		result := make([]any, 0, len(items))
		foundAny := false

		for _, item := range items {
			next, ok, err := walkSelection(item, remaining)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}

			foundAny = true
			if nested, ok := next.([]any); ok {
				result = append(result, nested...)
			} else {
				result = append(result, next)
			}
		}

		if !foundAny {
			return nil, false, nil
		}

		return result, true, nil
	}

	next, ok, err := applySelectorToken(value, token)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return walkSelection(next, remaining)
}

func walkDescendantSelection(value any, token selectorToken, remaining []selectorToken, allowCurrentMatch bool) (any, bool, error) {
	matches := make([]any, 0)
	foundAny := false

	if allowCurrentMatch && (token.kind != "field" || !isSelectorSequence(value)) {
		nextToken := selectorToken{
			kind:  token.kind,
			key:   token.key,
			index: token.index,
			op:    token.op,
			value: token.value,
		}

		next, ok, err := walkTokenMatch(value, nextToken, remaining)
		if err != nil {
			return nil, false, err
		}
		if ok {
			foundAny = true
			matches = appendSelectorMatches(matches, next)
		}
	}

	children := selectorChildren(value)
	if len(children) == 0 {
		if !foundAny {
			return nil, false, nil
		}
		if len(matches) == 1 {
			return matches[0], true, nil
		}
		return matches, true, nil
	}

	for _, child := range children {
		nested, ok, err := walkDescendantSelection(child, token, remaining, true)
		if err != nil {
			return nil, false, err
		}
		if ok {
			foundAny = true
			matches = appendSelectorMatches(matches, nested)
		}
	}

	if !foundAny {
		return nil, false, nil
	}

	if len(matches) == 1 {
		return matches[0], true, nil
	}

	return matches, true, nil
}

func appendSelectorMatches(target []any, value any) []any {
	if nested, ok := value.([]any); ok {
		return append(target, nested...)
	}
	return append(target, value)
}

func selectorChildren(value any) []any {
	if value == nil {
		return nil
	}

	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface {
		if current.IsNil() {
			return nil
		}
		current = current.Elem()
	}

	switch current.Kind() {
	case reflect.Map:
		if current.Type().Key().Kind() != reflect.String {
			return nil
		}

		keys := make([]string, 0, current.Len())
		for _, key := range current.MapKeys() {
			keys = append(keys, key.String())
		}
		sort.Strings(keys)

		children := make([]any, 0, len(keys))
		for _, key := range keys {
			child := current.MapIndex(reflect.ValueOf(key))
			if child.IsValid() {
				children = append(children, child.Interface())
			}
		}
		return children
	case reflect.Struct:
		children := make([]any, 0, current.NumField())
		for index := 0; index < current.NumField(); index++ {
			fieldValue := current.Field(index)
			if fieldValue.CanInterface() {
				children = append(children, fieldValue.Interface())
			}
		}
		return children
	default:
		children, _ := toSlice(value)
		return children
	}
}

func isSelectorSequence(value any) bool {
	_, ok := toSlice(value)
	return ok
}

func readSelectorGroup(input string, start int, open, close byte) (string, int, error) {
	if start >= len(input) || input[start] != open {
		return "", start, fmt.Errorf("selector group must start with %q", open)
	}

	quote := byte(0)
	for index := start + 1; index < len(input); index++ {
		char := input[index]
		switch {
		case quote != 0:
			if char == quote {
				quote = 0
			}
		case char == '"' || char == '\'':
			quote = char
		case char == close:
			return strings.TrimSpace(input[start+1 : index]), index + 1, nil
		}
	}

	return "", start, fmt.Errorf("unclosed selector group in %q", input)
}

func parseBracketSelector(content string) (selectorToken, error) {
	switch trimmed := strings.TrimSpace(content); trimmed {
	case "*":
		return selectorToken{kind: "wildcard"}, nil
	case "":
		return selectorToken{}, fmt.Errorf("empty selector bracket")
	default:
		if number, err := strconv.Atoi(trimmed); err == nil {
			return selectorToken{kind: "index", index: number}, nil
		}

		if strings.Contains(trimmed, "!=") {
			parts := strings.SplitN(trimmed, "!=", 2)
			return selectorToken{
				kind:  "attr",
				key:   strings.TrimSpace(parts[0]),
				op:    "!=",
				value: trimSelectorQuotes(parts[1]),
			}, nil
		}

		if strings.Contains(trimmed, "=") {
			parts := strings.SplitN(trimmed, "=", 2)
			return selectorToken{
				kind:  "attr",
				key:   strings.TrimSpace(parts[0]),
				op:    "=",
				value: trimSelectorQuotes(parts[1]),
			}, nil
		}

		return selectorToken{
			kind: "attr",
			key:  strings.TrimSpace(trimmed),
			op:   "exists",
		}, nil
	}
}

func parsePseudoSelector(input string, start int) (string, string, int, error) {
	index := start + 1
	for index < len(input) && (isSelectorNameChar(input[index]) || input[index] == '-') {
		index++
	}

	name := strings.TrimSpace(input[start+1 : index])
	if name == "" {
		return "", "", start, fmt.Errorf("empty pseudo selector in %q", input)
	}

	if index < len(input) && input[index] == '(' {
		arg, nextIndex, err := readSelectorGroup(input, index, '(', ')')
		if err != nil {
			return "", "", start, err
		}
		return name, arg, nextIndex, nil
	}

	return name, "", index, nil
}

func isSelectorNameChar(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_'
}

func isSelectorSpace(char byte) bool {
	return char == ' ' || char == '\t' || char == '\r' || char == '\n'
}

func trimSelectorQuotes(input string) string {
	input = strings.TrimSpace(input)
	if len(input) >= 2 {
		if (input[0] == '"' && input[len(input)-1] == '"') || (input[0] == '\'' && input[len(input)-1] == '\'') {
			return input[1 : len(input)-1]
		}
	}
	return input
}

func applyFieldSelector(value any, key string) (any, bool, error) {
	if items, ok := toSlice(value); ok {
		result := make([]any, 0, len(items))
		foundAny := false
		for _, item := range items {
			next, ok := lookupField(item, key)
			if !ok {
				continue
			}
			foundAny = true
			if nested, ok := next.([]any); ok {
				result = append(result, nested...)
			} else {
				result = append(result, next)
			}
		}

		if !foundAny {
			return nil, false, nil
		}

		return result, true, nil
	}

	next, ok := lookupField(value, key)
	return next, ok, nil
}

func applyAttributeSelector(value any, key, op, expected string) (any, bool, error) {
	if items, ok := toSlice(value); ok {
		filtered := make([]any, 0, len(items))
		for _, item := range items {
			matched, err := matchesAttribute(item, key, op, expected)
			if err != nil {
				return nil, false, err
			}
			if matched {
				filtered = append(filtered, item)
			}
		}
		if len(filtered) == 0 {
			return nil, false, nil
		}
		return filtered, true, nil
	}

	matched, err := matchesAttribute(value, key, op, expected)
	if err != nil {
		return nil, false, err
	}
	if !matched {
		return nil, false, nil
	}
	return value, true, nil
}

func matchesAttribute(value any, key, op, expected string) (bool, error) {
	attrValue, ok := lookupField(value, key)
	if !ok {
		return op == "!=", nil
	}

	switch op {
	case "exists":
		return true, nil
	case "=":
		return normalizeKey(attrValue) == expected, nil
	case "!=":
		return normalizeKey(attrValue) != expected, nil
	default:
		return false, fmt.Errorf("unsupported attribute selector op %q", op)
	}
}

func applyPseudoSelector(value any, name, arg string) (any, bool, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "first":
		items, ok := toSlice(value)
		if !ok || len(items) == 0 {
			return nil, false, nil
		}
		return items[0], true, nil
	case "last":
		items, ok := toSlice(value)
		if !ok || len(items) == 0 {
			return nil, false, nil
		}
		return items[len(items)-1], true, nil
	case "eq":
		items, ok := toSlice(value)
		if !ok {
			return nil, false, nil
		}
		index, err := strconv.Atoi(strings.TrimSpace(arg))
		if err != nil {
			return nil, false, fmt.Errorf(":eq expects an integer, got %q", arg)
		}
		if index < 0 || index >= len(items) {
			return nil, false, nil
		}
		return items[index], true, nil
	case "keys":
		keys, ok := mapKeys(value)
		if !ok {
			return nil, false, nil
		}
		return keys, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported pseudo selector :%s", name)
	}
}

func mapKeys(value any) ([]string, bool) {
	if value == nil {
		return nil, false
	}

	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface {
		if current.IsNil() {
			return nil, false
		}
		current = current.Elem()
	}

	if current.Kind() != reflect.Map || current.Type().Key().Kind() != reflect.String {
		return nil, false
	}

	keys := make([]string, 0, current.Len())
	for _, key := range current.MapKeys() {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	return keys, true
}

func lookupField(value any, key string) (any, bool) {
	if value == nil {
		return nil, false
	}

	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface {
		if current.IsNil() {
			return nil, false
		}
		current = current.Elem()
	}

	switch current.Kind() {
	case reflect.Map:
		if current.Type().Key().Kind() != reflect.String {
			return nil, false
		}

		found := current.MapIndex(reflect.ValueOf(key))
		if !found.IsValid() {
			return nil, false
		}
		return found.Interface(), true
	case reflect.Struct:
		for index := 0; index < current.NumField(); index++ {
			field := current.Type().Field(index)
			if strings.EqualFold(field.Name, key) {
				valueField := current.Field(index)
				if valueField.CanInterface() {
					return valueField.Interface(), true
				}
			}
		}
	}

	return nil, false
}

func toSlice(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}

	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface {
		if current.IsNil() {
			return nil, false
		}
		current = current.Elem()
	}

	if current.Kind() != reflect.Slice && current.Kind() != reflect.Array {
		return nil, false
	}

	items := make([]any, current.Len())
	for index := 0; index < current.Len(); index++ {
		items[index] = current.Index(index).Interface()
	}

	return items, true
}

func applyOperations(value any, operationText string) (any, error) {
	operations, err := splitOperations(operationText)
	if err != nil {
		return nil, err
	}

	current := value
	for _, rawOperation := range operations {
		name, arg := parseOperation(rawOperation)
		switch name {
		case "":
			continue
		case "trim":
			current = strings.TrimSpace(fmt.Sprint(current))
		case "upper":
			current = strings.ToUpper(fmt.Sprint(current))
		case "lower":
			current = strings.ToLower(fmt.Sprint(current))
		case "string":
			current = fmt.Sprint(current)
		case "int":
			number, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(current)), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("convert %q to int: %w", fmt.Sprint(current), err)
			}
			current = number
		case "float":
			number, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(current)), 64)
			if err != nil {
				return nil, fmt.Errorf("convert %q to float: %w", fmt.Sprint(current), err)
			}
			current = number
		case "bool":
			boolean, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(fmt.Sprint(current))))
			if err != nil {
				return nil, fmt.Errorf("convert %q to bool: %w", fmt.Sprint(current), err)
			}
			current = boolean
		case "json":
			current, err = parseJSONValue(current)
			if err != nil {
				return nil, err
			}
		case "json_string":
			encoded, err := json.Marshal(current)
			if err != nil {
				return nil, fmt.Errorf("marshal json string: %w", err)
			}
			current = string(encoded)
		case "split":
			parts := []string{}
			text := fmt.Sprint(current)
			if strings.TrimSpace(text) != "" {
				parts = strings.Split(text, arg)
			}
			current = parts
		case "join":
			items, ok := toSlice(current)
			if !ok {
				return nil, fmt.Errorf("join expects a slice, got %T", current)
			}
			stringItems := make([]string, 0, len(items))
			for _, item := range items {
				stringItems = append(stringItems, fmt.Sprint(item))
			}
			current = strings.Join(stringItems, arg)
		case "first":
			items, ok := toSlice(current)
			if !ok || len(items) == 0 {
				current = nil
				break
			}
			current = items[0]
		case "last":
			items, ok := toSlice(current)
			if !ok || len(items) == 0 {
				current = nil
				break
			}
			current = items[len(items)-1]
		case "flat":
			items, ok := toSlice(current)
			if !ok {
				return nil, fmt.Errorf("flat expects a slice, got %T", current)
			}
			flattened := make([]any, 0, len(items))
			for _, item := range items {
				if nested, ok := toSlice(item); ok {
					flattened = append(flattened, nested...)
				} else {
					flattened = append(flattened, item)
				}
			}
			current = flattened
		case "allow":
			if isEmpty(current) {
				break
			}

			allowed := strings.Split(arg, "|")
			currentValue := fmt.Sprint(current)
			matched := false
			for _, candidate := range allowed {
				if currentValue == candidate {
					matched = true
					break
				}
			}
			if !matched {
				return nil, fmt.Errorf("value %q is not allowed", currentValue)
			}
		default:
			return nil, fmt.Errorf("unsupported operation %q", name)
		}
	}

	return current, nil
}

func splitOperations(input string) ([]string, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}

	parts := make([]string, 0)
	depth := 0
	start := 0

	for index, runeValue := range input {
		switch runeValue {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("invalid operation list %q", input)
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(input[start:index]))
				start = index + 1
			}
		}
	}

	if depth != 0 {
		return nil, fmt.Errorf("invalid operation list %q", input)
	}

	parts = append(parts, strings.TrimSpace(input[start:]))
	return parts, nil
}

func parseOperation(input string) (string, string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", ""
	}

	start := strings.IndexByte(input, '(')
	end := strings.LastIndexByte(input, ')')
	if start < 0 || end < 0 || end < start {
		return input, ""
	}

	return strings.TrimSpace(input[:start]), input[start+1 : end]
}

func parseJSONValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil, nil
		}
		var parsed any
		if err := json.Unmarshal([]byte(typed), &parsed); err != nil {
			return nil, fmt.Errorf("parse json: %w", err)
		}
		return parsed, nil
	default:
		current := reflect.ValueOf(value)
		for current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface {
			if current.IsNil() {
				return nil, nil
			}
			current = current.Elem()
		}

		if current.Kind() == reflect.Map || current.Kind() == reflect.Slice || current.Kind() == reflect.Array || current.Kind() == reflect.Struct {
			return value, nil
		}

		return nil, fmt.Errorf("json operation expects a JSON string or structured value, got %T", value)
	}
}

func setObjectPath(target map[string]any, path string, value any) error {
	parts := strings.Split(strings.TrimSpace(path), ".")
	if len(parts) == 0 || parts[0] == "" {
		return fmt.Errorf("empty field path")
	}

	current := target
	for index, part := range parts {
		if part == "" {
			return fmt.Errorf("invalid field path %q", path)
		}

		if index == len(parts)-1 {
			current[part] = value
			return nil
		}

		next, ok := current[part]
		if !ok {
			child := make(map[string]any)
			current[part] = child
			current = child
			continue
		}

		nested, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("path %q conflicts with existing non-object field %q", path, part)
		}

		current = nested
	}

	return nil
}

func buildTree(items []any, idField, parentField, childrenKey, rootValue string) ([]any, error) {
	nodes := make(map[string]map[string]any, len(items))
	ordered := make([]map[string]any, 0, len(items))

	for _, item := range items {
		node, err := toObjectMap(item)
		if err != nil {
			return nil, err
		}

		idValue, ok := node[idField]
		if !ok {
			return nil, fmt.Errorf("tree item missing id field %q", idField)
		}

		clone := make(map[string]any, len(node)+1)
		for key, value := range node {
			clone[key] = value
		}
		clone[childrenKey] = []any{}

		nodes[normalizeKey(idValue)] = clone
		ordered = append(ordered, clone)
	}

	roots := make([]any, 0)
	for _, node := range ordered {
		parentValue, hasParent := node[parentField]
		if !hasParent || isRootValue(parentValue, rootValue) {
			roots = append(roots, node)
			continue
		}

		parentNode, ok := nodes[normalizeKey(parentValue)]
		if !ok {
			roots = append(roots, node)
			continue
		}

		children, _ := parentNode[childrenKey].([]any)
		parentNode[childrenKey] = append(children, node)
	}

	return roots, nil
}

func toObjectMap(value any) (map[string]any, error) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal tree item: %w", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			return nil, fmt.Errorf("decode tree item: %w", err)
		}

		return decoded, nil
	}
}

func normalizeKey(value any) string {
	return fmt.Sprint(value)
}

func isRootValue(value any, rootValue string) bool {
	if strings.TrimSpace(rootValue) != "" {
		return normalizeKey(value) == strings.TrimSpace(rootValue)
	}

	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(typed) == "" || typed == "0"
	case int:
		return typed == 0
	case int8:
		return typed == 0
	case int16:
		return typed == 0
	case int32:
		return typed == 0
	case int64:
		return typed == 0
	case float32:
		return typed == 0
	case float64:
		return typed == 0
	}

	return false
}

func plainSQLArgs(sqlText string, vars map[string]any) ([]any, error) {
	matches := sqlParamPattern.FindAllStringSubmatch(sqlText, -1)
	if len(matches) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(matches))
	args := make([]any, 0, len(matches))

	for _, match := range matches {
		name := match[1]
		if _, exists := seen[name]; exists {
			continue
		}

		value, ok := vars[name]
		if !ok {
			return nil, fmt.Errorf("sql references unknown parameter %q", name)
		}

		args = append(args, sql.Named(name, value))
		seen[name] = struct{}{}
	}

	return args, nil
}

func sortedParams(vars map[string]any) []ResolvedParam {
	keys := make([]string, 0, len(vars))
	for key := range vars {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	params := make([]ResolvedParam, 0, len(keys))
	for _, key := range keys {
		params = append(params, ResolvedParam{
			Name:  key,
			Value: vars[key],
		})
	}

	return params
}

func scanRows(rows *sql.Rows) (*QueryResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}

	result := &QueryResult{
		Columns: columns,
		Rows:    make([]map[string]any, 0),
	}

	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}

		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		row := make(map[string]any, len(columns))
		for index, column := range columns {
			row[column] = normalizeDBValue(values[index])
		}

		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return result, nil
}

func normalizeDBValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return typed
	}
}

func isEmpty(value any) bool {
	if value == nil {
		return true
	}

	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case []byte:
		return len(typed) == 0
	}

	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer || current.Kind() == reflect.Interface {
		if current.IsNil() {
			return true
		}
		current = current.Elem()
	}

	switch current.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice:
		return current.Len() == 0
	}

	return false
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
