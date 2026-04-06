package workflow

import (
	"encoding/xml"
	"fmt"
	"strings"
)

type Workflow struct {
	XMLName     xml.Name `xml:"workflow"`
	Name        string
	Title       string
	Description string
	Inputs      []Input
	Steps       []Step

	SourcePath string `xml:"-"`
	RawXML     string `xml:"-"`
}

type Input struct {
	XMLName     xml.Name `xml:"input"`
	Name        string   `xml:"name,attr"`
	Label       string   `xml:"label,attr"`
	Description string   `xml:"description,attr"`
	Placeholder string   `xml:"placeholder,attr"`
	Default     string   `xml:"default,attr"`
	Type        string   `xml:"type,attr"`
	Required    bool     `xml:"required,attr"`
	Text        string   `xml:",chardata"`
}

type Step struct {
	Kind        string
	Name        string
	From        string
	Path        string
	By          string
	Value       string
	Template    string
	Default     string
	Op          string
	Optional    bool
	Mode        string
	Datasource  string
	Engine      string
	IDField     string
	ParentField string
	ChildrenKey string
	Root        string
	Text        string
	Children    []Step
}

func (w *Workflow) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	*w = Workflow{XMLName: start.Name}

	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "name":
			w.Name = attr.Value
		case "title":
			w.Title = attr.Value
		case "description":
			w.Description = attr.Value
		}
	}

	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		switch typed := token.(type) {
		case xml.StartElement:
			switch typed.Name.Local {
			case "input":
				var input Input
				if err := decoder.DecodeElement(&input, &typed); err != nil {
					return err
				}
				w.Inputs = append(w.Inputs, input)
			case "var", "pick", "transform", "sql", "set":
				var step Step
				if err := decoder.DecodeElement(&step, &typed); err != nil {
					return err
				}
				w.Steps = append(w.Steps, step)
			default:
				return fmt.Errorf("workflow %q has unsupported tag <%s>", w.Name, typed.Name.Local)
			}
		case xml.EndElement:
			if typed.Name == start.Name {
				return nil
			}
		}
	}
}

func (s *Step) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	*s = Step{Kind: start.Name.Local}

	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "name":
			s.Name = attr.Value
		case "from":
			s.From = attr.Value
		case "path", "selector", "select":
			s.Path = attr.Value
		case "by":
			s.By = attr.Value
		case "value":
			s.Value = attr.Value
		case "template":
			s.Template = attr.Value
		case "default":
			s.Default = attr.Value
		case "op":
			s.Op = attr.Value
		case "optional":
			s.Optional = strings.EqualFold(attr.Value, "true")
		case "mode":
			s.Mode = attr.Value
		case "datasource":
			s.Datasource = attr.Value
		case "engine":
			s.Engine = attr.Value
		case "id":
			s.IDField = attr.Value
		case "parent":
			s.ParentField = attr.Value
		case "children":
			s.ChildrenKey = attr.Value
		case "root":
			s.Root = attr.Value
		}
	}

	var textBuilder strings.Builder

	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		switch typed := token.(type) {
		case xml.StartElement:
			var child Step
			if err := decoder.DecodeElement(&child, &typed); err != nil {
				return err
			}
			s.Children = append(s.Children, child)
		case xml.CharData:
			textBuilder.Write([]byte(typed))
		case xml.EndElement:
			if typed.Name == start.Name {
				s.Text = strings.TrimSpace(textBuilder.String())
				return nil
			}
		}
	}
}

func (w *Workflow) Validate() error {
	if strings.TrimSpace(w.Name) == "" {
		return fmt.Errorf("workflow name is required")
	}

	if strings.TrimSpace(w.Title) == "" {
		w.Title = w.Name
	}

	seenInputs := make(map[string]struct{}, len(w.Inputs))
	for _, input := range w.Inputs {
		if strings.TrimSpace(input.Name) == "" {
			return fmt.Errorf("workflow %q has an input without a name", w.Name)
		}

		if _, exists := seenInputs[input.Name]; exists {
			return fmt.Errorf("workflow %q has duplicate input %q", w.Name, input.Name)
		}

		seenInputs[input.Name] = struct{}{}
	}

	for _, step := range w.Steps {
		if err := validateStep(step, true, w.Name); err != nil {
			return err
		}
	}

	return nil
}

func validateStep(step Step, topLevel bool, workflowName string) error {
	switch step.Kind {
	case "var":
		if strings.TrimSpace(step.Name) == "" {
			return fmt.Errorf("workflow %q has a <var> without name", workflowName)
		}

		if len(step.Children) == 0 {
			sourceCount := nonEmptyCount(step.From, step.Value, step.Template, step.Text)
			if sourceCount == 0 && strings.TrimSpace(step.Op) == "" {
				return fmt.Errorf("<var name=%q> in workflow %q must define children or a direct expression", step.Name, workflowName)
			}
		}

		for _, child := range step.Children {
			if child.Kind != "pick" && child.Kind != "transform" && child.Kind != "set" {
				return fmt.Errorf("<var name=%q> in workflow %q only supports nested <pick>, <transform> or legacy <set>", step.Name, workflowName)
			}
			if strings.TrimSpace(child.Name) != "" {
				return fmt.Errorf("nested <%s> inside <var name=%q> in workflow %q must not define name", child.Kind, step.Name, workflowName)
			}
			if err := validateStep(child, false, workflowName); err != nil {
				return err
			}
		}

	case "pick":
		if topLevel && strings.TrimSpace(step.Name) == "" {
			return fmt.Errorf("top-level <pick> in workflow %q must define name", workflowName)
		}
		if len(step.Children) > 0 {
			return fmt.Errorf("<pick> in workflow %q does not support child steps", workflowName)
		}
		if topLevel && strings.TrimSpace(step.From) == "" && strings.TrimSpace(step.Path) == "" {
			return fmt.Errorf("top-level <pick name=%q> in workflow %q must define from or path", step.Name, workflowName)
		}

	case "transform":
		if topLevel && strings.TrimSpace(step.Name) == "" {
			return fmt.Errorf("top-level <transform> in workflow %q must define name", workflowName)
		}
		if step.TransformMode() == "js" {
			if len(step.Children) > 0 {
				return fmt.Errorf("<transform mode=js> in workflow %q does not support child steps", workflowName)
			}
			if strings.TrimSpace(step.Body()) == "" {
				return fmt.Errorf("<transform name=%q mode=js> in workflow %q must define script body", step.Name, workflowName)
			}
			break
		}
		if step.TransformMode() == "tree" {
			if len(step.Children) > 0 {
				return fmt.Errorf("<transform mode=%q> in workflow %q does not support child steps", step.TransformMode(), workflowName)
			}
			if strings.TrimSpace(step.IDField) == "" || strings.TrimSpace(step.ParentField) == "" {
				return fmt.Errorf("<transform name=%q mode=tree> in workflow %q must define id and parent", step.Name, workflowName)
			}
			break
		}
		if step.TransformMode() == "group" || step.TransformMode() == "index" {
			if strings.TrimSpace(step.By) == "" {
				return fmt.Errorf("<transform name=%q mode=%s> in workflow %q must define by", step.Name, step.TransformMode(), workflowName)
			}
		}

		if len(step.Children) > 0 {
			for _, child := range step.Children {
				if child.Kind != "field" {
					return fmt.Errorf("<transform name=%q> in workflow %q only supports nested <field>", step.Name, workflowName)
				}
				if err := validateStep(child, false, workflowName); err != nil {
					return err
				}
			}
		}

	case "set":
		if topLevel && strings.TrimSpace(step.Name) == "" {
			return fmt.Errorf("top-level <set> in workflow %q must define name", workflowName)
		}
		sourceCount := nonEmptyCount(step.From, step.Value, step.Template)
		if sourceCount != 1 {
			return fmt.Errorf("<set name=%q> in workflow %q must define exactly one of from, value or template", step.Name, workflowName)
		}

	case "sql":
		if !topLevel {
			return fmt.Errorf("workflow %q does not allow nested <sql>", workflowName)
		}
		if strings.TrimSpace(step.Text) == "" {
			return fmt.Errorf("workflow %q must define SQL text", workflowName)
		}

		engine := step.NormalizedEngine()
		if engine != "plain" && engine != "gosql" {
			return fmt.Errorf("workflow %q has unsupported sql engine %q", workflowName, step.Engine)
		}

		mode := step.NormalizedMode()
		if mode != "query" && mode != "exec" {
			return fmt.Errorf("workflow %q has unsupported sql mode %q", workflowName, step.Mode)
		}

	case "field":
		if topLevel {
			return fmt.Errorf("workflow %q does not allow top-level <field>", workflowName)
		}
		if len(step.Children) > 0 {
			return fmt.Errorf("<field> in workflow %q does not support child steps", workflowName)
		}
		if strings.TrimSpace(step.TargetPath()) == "" {
			return fmt.Errorf("<field> in workflow %q must define path or name", workflowName)
		}

	default:
		return fmt.Errorf("workflow %q has unsupported tool <%s>", workflowName, step.Kind)
	}

	return nil
}

func nonEmptyCount(values ...string) int {
	count := 0
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			count++
		}
	}
	return count
}

func (input Input) InitialValue() string {
	if input.Default != "" {
		return input.Default
	}
	return strings.TrimSpace(input.Text)
}

func (input Input) IsMultiline() bool {
	return strings.EqualFold(input.Type, "json") || strings.EqualFold(input.Type, "textarea")
}

func (step Step) Body() string {
	if strings.TrimSpace(step.Template) != "" {
		return step.Template
	}
	return strings.TrimSpace(step.Text)
}

func (step Step) TargetPath() string {
	if strings.TrimSpace(step.Path) != "" {
		return step.Path
	}
	return step.Name
}

func (step Step) NormalizedMode() string {
	mode := strings.TrimSpace(strings.ToLower(step.Mode))
	if mode == "" {
		return "query"
	}
	return mode
}

func (step Step) TransformMode() string {
	mode := strings.TrimSpace(strings.ToLower(step.Mode))
	if mode == "" && len(step.Children) > 0 {
		return "object"
	}
	return mode
}

func (step Step) NormalizedEngine() string {
	engine := strings.TrimSpace(strings.ToLower(step.Engine))
	if engine == "" {
		return "plain"
	}
	return engine
}
