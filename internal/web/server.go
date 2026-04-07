package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"

	"github.com/llyb120/go-future/internal/workflow"
)

//go:embed templates/*.html
var templateFS embed.FS

type Server struct {
	catalog  *workflow.Catalog
	executor *workflow.Executor
	tmpl     *template.Template
}

type PageData struct {
	Workflows []*workflow.Workflow
	Selected  *workflow.Workflow
	Values    map[string]string
	Result    *workflow.Execution
	Error     string
}

func NewServer(catalog *workflow.Catalog, executor *workflow.Executor) (*Server, error) {
	funcMap := template.FuncMap{
		"value": func(values map[string]string, input workflow.Input) string {
			if values == nil {
				return input.InitialValue()
			}

			if value, ok := values[input.Name]; ok {
				return value
			}

			return input.InitialValue()
		},
		"multiline": func(input workflow.Input) bool {
			return input.IsMultiline()
		},
		"display": func(value any) string {
			if value == nil {
				return ""
			}

			switch value.(type) {
			case map[string]any, []any, []string:
				encoded, err := json.Marshal(value)
				if err == nil {
					return string(encoded)
				}
			}

			return fmt.Sprint(value)
		},
	}

	tmpl, err := template.New("index.html").Funcs(funcMap).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}

	return &Server{
		catalog:  catalog,
		executor: executor,
		tmpl:     tmpl,
	}, nil
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("POST /run", s.handleRun)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	selected, err := s.selectWorkflow(r.URL.Query().Get("workflow"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	s.renderPage(w, PageData{
		Workflows: s.catalog.List(),
		Selected:  selected,
		Values:    map[string]string{},
	})
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "parse form failed", http.StatusBadRequest)
		return
	}

	selected, err := s.selectWorkflow(r.Form.Get("workflow"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	values := make(map[string]string, len(selected.Inputs))
	for _, input := range selected.Inputs {
		values[input.Name] = r.Form.Get(input.Name)
	}

	result, execErr := s.executor.Run(r.Context(), selected, values)

	data := PageData{
		Workflows: s.catalog.List(),
		Selected:  selected,
		Values:    values,
		Result:    result,
	}

	if execErr != nil {
		data.Error = execErr.Error()
	}

	s.renderPage(w, data)
}

func (s *Server) renderPage(w http.ResponseWriter, data PageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) selectWorkflow(name string) (*workflow.Workflow, error) {
	if name == "" {
		selected := s.catalog.Default()
		if selected == nil {
			return nil, fmt.Errorf("no workflows available")
		}
		return selected, nil
	}

	selected, ok := s.catalog.Get(name)
	if !ok {
		return nil, fmt.Errorf("workflow %q not found", name)
	}

	return selected, nil
}
