package benchmarksync

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func (d *sourceDifficulty) UnmarshalJSON(data []byte) error {
	type sourceDifficultyAlias struct {
		DifficultyName     string           `json:"difficultyName"`
		KovaaksBenchmarkID int64            `json:"kovaaksBenchmarkId"`
		Sharecode          string           `json:"sharecode"`
		RankColors         json.RawMessage  `json:"rankColors"`
		Categories         []sourceCategory `json:"categories"`
	}

	var aux sourceDifficultyAlias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	rankColors, err := parseOrderedRankColors(aux.RankColors)
	if err != nil {
		return fmt.Errorf("parse rankColors: %w", err)
	}

	d.DifficultyName = aux.DifficultyName
	d.KovaaksBenchmarkID = aux.KovaaksBenchmarkID
	d.Sharecode = aux.Sharecode
	d.RankColors = rankColors
	d.Categories = aux.Categories

	return nil
}

func parseOrderedRankColors(raw json.RawMessage) ([]sourceRankColor, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil, fmt.Errorf("rankColors must be an object")
	}

	rankColors := make([]sourceRankColor, 0)
	for dec.More() {
		nameTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name, ok := nameTok.(string)
		if !ok {
			return nil, fmt.Errorf("rankColors key must be a string")
		}

		var colorRaw any
		if err := dec.Decode(&colorRaw); err != nil {
			return nil, err
		}

		color, ok := colorRaw.(string)
		if !ok {
			continue
		}

		rankColors = append(rankColors, sourceRankColor{
			Name:  name,
			Color: color,
		})
	}

	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	if len(rankColors) == 0 {
		return nil, nil
	}

	return rankColors, nil
}
