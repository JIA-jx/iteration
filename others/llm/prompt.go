package llm

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type PromptTemplate struct {
	Name         string
	Version      string
	Template     string
	RequiredVars []string
	Description  string
}

var (
	promptMu       sync.RWMutex
	promptRegistry = make(map[string]PromptTemplate)
)

func RegisterPrompt(t PromptTemplate) error {
	if t.Name == "" {
		return errors.New("llm: prompt name is empty")
	}
	if t.Version == "" {
		t.Version = "v1"
	}
	if strings.TrimSpace(t.Template) == "" {
		return errors.New("llm: prompt template is empty")
	}

	promptMu.Lock()
	defer promptMu.Unlock()
	promptRegistry[promptKey(t.Name, t.Version)] = t
	return nil
}

func GetPrompt(name, version string) (PromptTemplate, bool) {
	if version == "" {
		version = "v1"
	}
	promptMu.RLock()
	defer promptMu.RUnlock()
	t, ok := promptRegistry[promptKey(name, version)]
	return t, ok
}

func Render(name, version string, vars map[string]string) (string, error) {
	t, ok := GetPrompt(name, version)
	if !ok {
		return "", fmt.Errorf("llm: prompt %s@%s not registered", name, version)
	}

	missing := make([]string, 0, len(t.RequiredVars))
	for _, v := range t.RequiredVars {
		if _, exists := vars[v]; !exists {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("llm: prompt %s@%s missing vars: %s", name, version, strings.Join(missing, ","))
	}

	out := t.Template
	for k, v := range vars {
		out = strings.ReplaceAll(out, "{{"+k+"}}", v)
	}
	return out, nil
}

func LatestVersion(name string) string {
	promptMu.RLock()
	defer promptMu.RUnlock()

	versions := make([]string, 0, 4)
	for _, t := range promptRegistry {
		if t.Name == name {
			versions = append(versions, t.Version)
		}
	}
	if len(versions) == 0 {
		return ""
	}
	sort.Strings(versions)
	return versions[len(versions)-1]
}

func ListPrompts() []PromptTemplate {
	promptMu.RLock()
	defer promptMu.RUnlock()

	out := make([]PromptTemplate, 0, len(promptRegistry))
	for _, t := range promptRegistry {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Version < out[j].Version
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func promptKey(name, version string) string {
	return name + "@" + version
}
