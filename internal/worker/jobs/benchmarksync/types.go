package benchmarksync

import (
	"encoding/json"
	"fmt"
	"strings"
)

type sourceBenchmark struct {
	BenchmarkName   string             `json:"benchmarkName"`
	RankCalculation string             `json:"rankCalculation"`
	Abbreviation    string             `json:"abbreviation"`
	Color           string             `json:"color"`
	SpreadsheetURL  string             `json:"spreadsheetURL"`
	DateAdded       string             `json:"dateAdded"`
	Difficulties    []sourceDifficulty `json:"difficulties"`
}

type sourceDifficulty struct {
	DifficultyName     string            `json:"difficultyName"`
	KovaaksBenchmarkID int64             `json:"kovaaksBenchmarkId"`
	Sharecode          string            `json:"sharecode"`
	RankColors         map[string]string `json:"rankColors"`
	Categories         []sourceCategory  `json:"categories"`
	Scenarios          scenarioRefList   `json:"scenarios"`
}

type sourceCategory struct {
	CategoryName  string              `json:"categoryName"`
	Color         string              `json:"color"`
	Subcategories []sourceSubcategory `json:"subcategories"`
	Scenarios     scenarioRefList     `json:"scenarios"`
}

type sourceSubcategory struct {
	SubcategoryName string          `json:"subcategoryName"`
	ScenarioCount   int             `json:"scenarioCount"`
	Color           string          `json:"color"`
	Scenarios       scenarioRefList `json:"scenarios"`
	ScenarioNames   []string        `json:"scenarioNames"`
}

type scenarioRef struct {
	Name   string
	Weight float64
}

type scenarioRefList []scenarioRef

type scenarioRefObject struct {
	Name         string   `json:"name"`
	Scenario     string   `json:"scenario"`
	ScenarioName string   `json:"scenarioName"`
	Weight       *float64 `json:"weight"`
}

func (l *scenarioRefList) UnmarshalJSON(data []byte) error {
	data = bytesTrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		*l = nil
		return nil
	}

	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return fmt.Errorf("scenarios must be an array: %w", err)
	}

	out := make([]scenarioRef, 0, len(rawItems))
	for _, raw := range rawItems {
		raw = bytesTrimSpace(raw)
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}

		var asString string
		if err := json.Unmarshal(raw, &asString); err == nil {
			name := strings.TrimSpace(asString)
			if name == "" {
				continue
			}
			out = append(out, scenarioRef{Name: name, Weight: 1})
			continue
		}

		var asObject scenarioRefObject
		if err := json.Unmarshal(raw, &asObject); err != nil {
			return fmt.Errorf("invalid scenario ref entry %s: %w", string(raw), err)
		}

		name := strings.TrimSpace(firstNonEmpty(asObject.Name, asObject.ScenarioName, asObject.Scenario))
		if name == "" {
			continue
		}
		weight := 1.0
		if asObject.Weight != nil && *asObject.Weight > 0 {
			weight = *asObject.Weight
		}

		out = append(out, scenarioRef{Name: name, Weight: weight})
	}

	*l = out
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func bytesTrimSpace(data []byte) []byte {
	start := 0
	for start < len(data) {
		if data[start] != ' ' && data[start] != '\n' && data[start] != '\r' && data[start] != '\t' {
			break
		}
		start++
	}
	end := len(data)
	for end > start {
		if data[end-1] != ' ' && data[end-1] != '\n' && data[end-1] != '\r' && data[end-1] != '\t' {
			break
		}
		end--
	}
	return data[start:end]
}
