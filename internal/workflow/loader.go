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

	catalog := &Catalog{
		list:   make([]*Workflow, 0, len(matches)),
		byName: make(map[string]*Workflow, len(matches)),
	}

	for _, file := range matches {
		content, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read workflow file %s: %w", file, err)
		}

		var wf Workflow
		if err := xml.Unmarshal(content, &wf); err != nil {
			return nil, fmt.Errorf("parse workflow file %s: %w", file, err)
		}

		wf.SourcePath = file
		wf.RawXML = string(content)

		if err := wf.Validate(); err != nil {
			return nil, fmt.Errorf("validate workflow file %s: %w", file, err)
		}

		if _, exists := catalog.byName[wf.Name]; exists {
			return nil, fmt.Errorf("duplicate workflow name %q", wf.Name)
		}

		catalog.byName[wf.Name] = &wf
		catalog.list = append(catalog.list, &wf)
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
