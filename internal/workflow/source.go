package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

type markdownFence struct {
	heading  string
	language string
	body     string
}

func resolveStepBody(wf *Workflow, step Step, expectedKind string) (string, string, error) {
	if !step.HasSourceRef() {
		if expectedKind == "js" && !step.HasInlineBody() && strings.TrimSpace(step.Entry) != "" {
			if wf == nil || wf.resources == nil {
				return "", "", fmt.Errorf("%s entry %q requires preloaded javascript resources from LoadDir", stepLabel(step), step.Entry)
			}
			if bundle, ok := wf.resources.resolvePreloadedJS(step.Entry); ok {
				return bundle, "js-preloaded", nil
			}
			return "", "", fmt.Errorf("%s entry %q not found in preloaded javascript resources", stepLabel(step), step.Entry)
		}
		return step.Body(), inferInlineLanguage(step, expectedKind), nil
	}

	if step.HasInlineBody() {
		return "", "", fmt.Errorf("%s cannot define both src and inline body", stepLabel(step))
	}

	if expectedKind == "sql" && isNamedResourceReference(step.Source) {
		if wf == nil || wf.resources == nil {
			return "", "", fmt.Errorf("%s named sql source %q requires preloaded markdown resources from LoadDir", stepLabel(step), step.Source)
		}
		if body, language, ok := wf.resources.resolveSQLSource(step.Source); ok {
			return body, language, nil
		}
		return "", "", fmt.Errorf("%s sql source %q not found in preloaded markdown resources", stepLabel(step), step.Source)
	}

	resolvedPath, fragment := resolveStepSourcePath(wf, step.Source)
	content, err := os.ReadFile(resolvedPath)
	if err != nil {
		return "", "", fmt.Errorf("%s read src %q: %w", stepLabel(step), step.Source, err)
	}

	ext := strings.ToLower(filepath.Ext(resolvedPath))
	switch ext {
	case ".js", ".mjs", ".cjs":
		if expectedKind != "js" {
			return "", "", fmt.Errorf("%s src %q must point to SQL content", stepLabel(step), step.Source)
		}
		return strings.TrimSpace(string(content)), "js", nil
	case ".md", ".markdown":
		body, language, err := extractMarkdownSource(string(content), fragment, expectedKind, step.NormalizedEngine())
		if err != nil {
			return "", "", fmt.Errorf("%s load markdown src %q: %w", stepLabel(step), step.Source, err)
		}
		return body, language, nil
	default:
		if expectedKind == "sql" {
			return "", "", fmt.Errorf("%s src %q must be a markdown file", stepLabel(step), step.Source)
		}
		return "", "", fmt.Errorf("%s src %q has unsupported extension %q", stepLabel(step), step.Source, ext)
	}
}

func isNamedResourceReference(source string) bool {
	source = strings.TrimSpace(source)
	if source == "" {
		return false
	}

	pathPart, _, _ := strings.Cut(source, "#")
	if strings.ContainsAny(pathPart, `\/`) {
		return false
	}

	ext := strings.ToLower(filepath.Ext(pathPart))
	switch ext {
	case ".md", ".markdown", ".js", ".mjs", ".cjs", ".sql":
		return false
	}

	return strings.Contains(pathPart, ".")
}

func resolveStepSourcePath(wf *Workflow, source string) (string, string) {
	pathPart, fragment, _ := strings.Cut(strings.TrimSpace(source), "#")
	pathPart = filepath.FromSlash(pathPart)
	if filepath.IsAbs(pathPart) || wf == nil || strings.TrimSpace(wf.SourcePath) == "" {
		return pathPart, strings.TrimSpace(fragment)
	}

	baseDir := filepath.Dir(wf.SourcePath)
	return filepath.Join(baseDir, pathPart), strings.TrimSpace(fragment)
}

func extractMarkdownSource(markdown string, fragment string, expectedKind string, preferredEngine string) (string, string, error) {
	fences := collectMarkdownFences(markdown)
	if len(fences) == 0 {
		return "", "", fmt.Errorf("no fenced code blocks found")
	}

	if strings.TrimSpace(fragment) != "" {
		filtered := make([]markdownFence, 0, len(fences))
		for _, fence := range fences {
			if headingMatchesFragment(fence.heading, fragment) {
				filtered = append(filtered, fence)
			}
		}
		fences = filtered
		if len(fences) == 0 {
			return "", "", fmt.Errorf("no fenced code block found under heading %q", fragment)
		}
	}

	for _, language := range preferredMarkdownLanguages(expectedKind, preferredEngine) {
		for _, fence := range fences {
			if fence.language == language {
				return strings.TrimSpace(fence.body), fence.language, nil
			}
		}
	}

	return "", "", fmt.Errorf("no %s fenced code block found", expectedKind)
}

func collectMarkdownFences(markdown string) []markdownFence {
	lines := strings.Split(markdown, "\n")
	fences := make([]markdownFence, 0)
	currentHeading := ""
	inFence := false
	fenceLanguage := ""
	var body strings.Builder

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if !inFence {
			if heading, ok := parseMarkdownHeading(trimmed); ok {
				currentHeading = heading
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
			fences = append(fences, markdownFence{
				heading:  currentHeading,
				language: fenceLanguage,
				body:     body.String(),
			})
			inFence = false
			fenceLanguage = ""
			continue
		}

		if body.Len() > 0 {
			body.WriteByte('\n')
		}
		body.WriteString(line)
	}

	return fences
}

func parseMarkdownHeading(line string) (string, bool) {
	if line == "" || !strings.HasPrefix(line, "#") {
		return "", false
	}

	index := 0
	for index < len(line) && line[index] == '#' {
		index++
	}
	if index == 0 || index >= len(line) || line[index] != ' ' {
		return "", false
	}

	heading := strings.TrimSpace(line[index+1:])
	if heading == "" {
		return "", false
	}

	return heading, true
}

func parseFenceLanguage(line string) string {
	info := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "```"))
	if info == "" {
		return ""
	}

	fields := strings.Fields(info)
	if len(fields) == 0 {
		return ""
	}

	return strings.ToLower(fields[0])
}

func preferredMarkdownLanguages(expectedKind string, preferredEngine string) []string {
	switch expectedKind {
	case "js":
		return []string{"js", "javascript"}
	case "sql":
		return []string{"sql"}
	default:
		return nil
	}
}

func inferInlineLanguage(step Step, expectedKind string) string {
	switch expectedKind {
	case "js":
		return "js"
	case "sql":
		if step.NormalizedEngine() == "gosql" {
			return "gosql"
		}
		return "sql"
	default:
		return ""
	}
}

func effectiveSQLEngine(step Step, sourceLanguage string) string {
	_ = sourceLanguage
	if step.NormalizedEngine() == "gosql" {
		return "gosql"
	}
	return "plain"
}

func headingMatchesFragment(heading string, fragment string) bool {
	normalizedHeading := normalizeMarkdownFragment(heading)
	normalizedFragment := normalizeMarkdownFragment(fragment)
	return normalizedHeading != "" && normalizedHeading == normalizedFragment
}

func normalizeMarkdownFragment(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}

	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r), unicode.IsNumber(r):
			builder.WriteRune(r)
			lastDash = false
		case r == '-', r == '_', unicode.IsSpace(r):
			if !lastDash && builder.Len() > 0 {
				builder.WriteByte('-')
				lastDash = true
			}
		}
	}

	return strings.Trim(builder.String(), "-")
}

func stepLabel(step Step) string {
	if strings.TrimSpace(step.Name) != "" {
		return fmt.Sprintf("<%s name=%q>", step.Kind, step.Name)
	}

	return fmt.Sprintf("<%s>", step.Kind)
}
