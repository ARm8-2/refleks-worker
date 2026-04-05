package benchmarksync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func parseProgressScenarioDefinitions(raw []byte) ([]progressScenarioDefinition, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("progress: expected object start")
	}

	definitions := make([]progressScenarioDefinition, 0)
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := keyToken.(string)

		switch key {
		case "categories":
			if err := parseProgressCategories(dec, &definitions); err != nil {
				return nil, err
			}
		default:
			var discard any
			if err := dec.Decode(&discard); err != nil {
				return nil, err
			}
		}
	}

	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	return definitions, nil
}

func parseProgressCategories(dec *json.Decoder, definitions *[]progressScenarioDefinition) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delimiter, ok := tok.(json.Delim)
	if !ok || delimiter != '{' {
		return fmt.Errorf("categories: expected '{'")
	}

	for dec.More() {
		if _, err := dec.Token(); err != nil {
			return err
		}

		tok, err := dec.Token()
		if err != nil {
			return err
		}
		delimiter, ok := tok.(json.Delim)
		if !ok || delimiter != '{' {
			return fmt.Errorf("categories: expected category object")
		}

		for dec.More() {
			fieldToken, err := dec.Token()
			if err != nil {
				return err
			}
			field, _ := fieldToken.(string)
			if field == "scenarios" {
				if err := parseProgressScenarios(dec, definitions); err != nil {
					return err
				}
				continue
			}

			var discard any
			if err := dec.Decode(&discard); err != nil {
				return err
			}
		}

		if _, err := dec.Token(); err != nil {
			return err
		}
	}

	_, err = dec.Token()
	return err
}

func parseProgressScenarios(dec *json.Decoder, definitions *[]progressScenarioDefinition) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delimiter, ok := tok.(json.Delim)
	if !ok || delimiter != '{' {
		return fmt.Errorf("scenarios: expected '{'")
	}

	for dec.More() {
		nameToken, err := dec.Token()
		if err != nil {
			return err
		}
		name, _ := nameToken.(string)
		name = strings.TrimSpace(name)

		var scenarioPayload json.RawMessage
		if err := dec.Decode(&scenarioPayload); err != nil {
			return err
		}

		if name == "" {
			continue
		}

		rankThresholds, err := extractRankThresholds(scenarioPayload)
		if err != nil {
			return fmt.Errorf("parse rank thresholds for %q: %w", name, err)
		}

		*definitions = append(*definitions, progressScenarioDefinition{
			Name:           name,
			RankThresholds: rankThresholds,
		})
	}

	_, err = dec.Token()
	return err
}

func extractRankThresholds(rawScenario json.RawMessage) ([]float64, error) {
	if len(rawScenario) == 0 {
		return nil, nil
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(rawScenario, &payload); err != nil {
		return nil, err
	}

	rawRankMaxes, ok := payload["rank_maxes"]
	if !ok || len(rawRankMaxes) == 0 {
		return nil, nil
	}

	dec := json.NewDecoder(bytes.NewReader(rawRankMaxes))
	dec.UseNumber()

	var rawAny any
	if err := dec.Decode(&rawAny); err != nil {
		return nil, err
	}

	return normalizeThresholdsFromAny(rawAny), nil
}

func normalizeThresholdsFromAny(raw any) []float64 {
	out := make([]float64, 0)

	appendValue := func(value float64) {
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return
		}
		out = append(out, value)
	}

	switch typed := raw.(type) {
	case []any:
		for _, entry := range typed {
			if value, ok := coerceThresholdValue(entry); ok {
				appendValue(value)
			}
		}
	default:
		if value, ok := coerceThresholdValue(typed); ok {
			appendValue(value)
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

func coerceThresholdValue(raw any) (float64, bool) {
	switch typed := raw.(type) {
	case float64:
		return typed, true
	case json.Number:
		value, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return value, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		value, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return value, true
	default:
		return 0, false
	}
}
