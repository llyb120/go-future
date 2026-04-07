package workflow

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type Catalog struct {
	list   []*Workflow
	byName map[string]*Workflow
}

func LoadDir(dir string) (*Catalog, error) {
	pattern := filepath.Join(dir, "*.xml")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob workflow files: %w", err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no workflow xml files found in %s", dir)
	}

	workflows := make([]*Workflow, 0, len(matches))
	for _, file := range matches {
		wf, err := LoadFile(file)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, wf)
	}

	return NewCatalog(workflows...)
}

func LoadFile(path string) (*Workflow, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read workflow file %s: %w", path, err)
	}

	wf, err := Parse(content, path)
	if err != nil {
		return nil, fmt.Errorf("load workflow file %s: %w", path, err)
	}

	return wf, nil
}

func Parse(content []byte, sourcePath string) (*Workflow, error) {
	var wf Workflow
	if err := xml.Unmarshal(content, &wf); err != nil {
		return nil, fmt.Errorf("parse workflow xml: %w", err)
	}

	wf.SourcePath = sourcePath
	wf.RawXML = string(content)

	if err := wf.Validate(); err != nil {
		return nil, err
	}

	return &wf, nil
}

func ParseString(content string, sourcePath string) (*Workflow, error) {
	return Parse([]byte(content), sourcePath)
}

func NewCatalog(workflows ...*Workflow) (*Catalog, error) {
	catalog := &Catalog{
		list:   make([]*Workflow, 0, len(workflows)),
		byName: make(map[string]*Workflow, len(workflows)),
	}

	for _, wf := range workflows {
		if wf == nil {
			return nil, fmt.Errorf("catalog contains nil workflow")
		}
		if err := wf.Validate(); err != nil {
			return nil, fmt.Errorf("validate workflow %q: %w", wf.Name, err)
		}
		if _, exists := catalog.byName[wf.Name]; exists {
			return nil, fmt.Errorf("duplicate workflow name %q", wf.Name)
		}

		catalog.byName[wf.Name] = wf
		catalog.list = append(catalog.list, wf)
	}

	sort.Slice(catalog.list, func(i, j int) bool {
		return catalog.list[i].Name < catalog.list[j].Name
	})

	return catalog, nil
}

func (c *Catalog) List() []*Workflow {
	if c == nil {
		return nil
	}

	out := make([]*Workflow, len(c.list))
	copy(out, c.list)
	return out
}

func (c *Catalog) Get(name string) (*Workflow, bool) {
	if c == nil {
		return nil, false
	}

	wf, ok := c.byName[name]
	return wf, ok
}

func (c *Catalog) Default() *Workflow {
	if c == nil || len(c.list) == 0 {
		return nil
	}

	return c.list[0]
}
