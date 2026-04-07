package workflow

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type resourceRegistry struct {
	sqlByName map[string]registeredSource
	jsBundle  string
	jsByName  map[string]string
}

type registeredSource struct {
	body     string
	language string
	path     string
}

func loadResourceRegistry(dir string) (*resourceRegistry, error) {
	registry := &resourceRegistry{
		sqlByName: make(map[string]registeredSource),
		jsByName:  make(map[string]string),
	}

	jsSources := make([]string, 0)
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != dir {
				hasWorkflowXML, err := dirContainsWorkflowXML(path)
				if err != nil {
					return err
				}
				if hasWorkflowXML {
					return filepath.SkipDir
				}
			}
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read resource %s: %w", path, err)
		}

		switch {
		case isPreloadedSQLMarkdownPath(path):
			if err := registry.registerMarkdownSQL(path, string(content)); err != nil {
				return err
			}
		case isPreloadedJSSourcePath(path):
			scriptSource, err := registry.registerJSSource(path, string(content))
			if err != nil {
				return err
			}
			if strings.TrimSpace(scriptSource) != "" {
				jsSources = append(jsSources, scriptSource)
			}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan workflow resources: %w", err)
	}

	registry.jsBundle = strings.Join(jsSources, "\n\n")
	return registry, nil
}

func isPreloadedSQLMarkdownPath(path string) bool {
	lower := strings.ToLower(filepath.ToSlash(path))
	return strings.HasSuffix(lower, ".sql.md") || strings.HasSuffix(lower, ".sql.markdown")
}

func isPreloadedJSSourcePath(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".js", ".mjs", ".cjs":
		return true
	default:
		return false
	}
}

func dirContainsWorkflowXML(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("read workflow directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".xml") {
			return true, nil
		}
	}

	return false, nil
}

func (r *resourceRegistry) registerMarkdownSQL(path string, markdown string) error {
	lines := strings.Split(markdown, "\n")
	namespace := ""
	statementName := ""
	inFence := false
	fenceLanguage := ""
	var body strings.Builder

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if !inFence {
			if level, heading, ok := parseMarkdownHeadingLevel(trimmed); ok {
				switch level {
				case 1:
					namespace = normalizeMarkdownFragment(heading)
					statementName = ""
				case 2:
					statementName = normalizeMarkdownFragment(heading)
				}
				continue
			}
			if strings.HasPrefix(trimmed, "```") {
				inFence = true
				fenceLanguage = parseFenceLanguage(trimmed)
				body.Reset()
			}
			continue
		}

		if strings.HasPrefix(trimmed, "```") {
			if fenceLanguage == "sql" && namespace != "" && statementName != "" {
				key := namespace + "." + statementName
				if err := r.registerSQLSource(key, strings.TrimSpace(body.String()), path); err != nil {
					return err
				}
			}
			inFence = false
			fenceLanguage = ""
			continue
		}

		if body.Len() > 0 {
			body.WriteByte('\n')
		}
		body.WriteString(line)
	}

	return nil
}

func (r *resourceRegistry) registerSQLSource(key string, body string, path string) error {
	if body == "" {
		return nil
	}
	if existing, exists := r.sqlByName[key]; exists {
		return fmt.Errorf("duplicate sql source %q in %s and %s", key, existing.path, path)
	}
	r.sqlByName[key] = registeredSource{
		body:     body,
		language: "sql",
		path:     path,
	}
	return nil
}

func (r *resourceRegistry) registerJSSource(path string, source string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", nil
	}

	exportedNames := collectExportedJSFunctionNames(source)
	hostExportNames := collectHostExportNames(source)
	for _, name := range append(exportedNames, hostExportNames...) {
		if existing, exists := r.jsByName[name]; exists {
			return "", fmt.Errorf("duplicate javascript function %q in %s and %s", name, existing, path)
		}
		r.jsByName[name] = path
	}

	if len(exportedNames) > 0 {
		scriptSource, _, err := buildExportedJSTransformScript(source, exportedNames[0])
		if err != nil {
			return "", fmt.Errorf("prepare javascript resource %s: %w", path, err)
		}
		return "(function(){\n" + scriptSource + "\n})();", nil
	}

	return source, nil
}

func (r *resourceRegistry) resolveSQLSource(reference string) (string, string, bool) {
	if r == nil {
		return "", "", false
	}

	key := normalizeResourceReference(reference)
	source, ok := r.sqlByName[key]
	if !ok {
		return "", "", false
	}

	return source.body, source.language, true
}

func (r *resourceRegistry) resolvePreloadedJS(entry string) (string, bool) {
	if r == nil {
		return "", false
	}
	if _, ok := r.jsByName[strings.TrimSpace(entry)]; !ok {
		return "", false
	}
	if strings.TrimSpace(r.jsBundle) == "" {
		return "", false
	}
	return r.jsBundle, true
}

func normalizeResourceReference(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ".")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = normalizeMarkdownFragment(part)
		if part != "" {
			normalized = append(normalized, part)
		}
	}
	return strings.Join(normalized, ".")
}

func parseMarkdownHeadingLevel(line string) (int, string, bool) {
	if line == "" || !strings.HasPrefix(line, "#") {
		return 0, "", false
	}

	level := 0
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level >= len(line) || line[level] != ' ' {
		return 0, "", false
	}

	heading := strings.TrimSpace(line[level+1:])
	if heading == "" {
		return 0, "", false
	}

	return level, heading, true
}
