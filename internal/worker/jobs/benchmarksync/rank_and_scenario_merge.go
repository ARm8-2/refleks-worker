package benchmarksync

import "strings"

func mergedRanksFromSource(rankColors []sourceRankColor) []mergedRank {
	if len(rankColors) == 0 {
		return nil
	}

	merged := make([]mergedRank, 0, len(rankColors))
	for _, rank := range rankColors {
		name := strings.TrimSpace(rank.Name)
		color := strings.TrimSpace(rank.Color)
		if name == "" || color == "" {
			continue
		}
		merged = append(merged, mergedRank{
			Name:      name,
			Color:     color,
			SortOrder: len(merged),
		})
	}

	if len(merged) == 0 {
		return nil
	}

	return merged
}

func applyProgressScenariosToDifficulty(difficulty sourceDifficulty, progressScenarios []progressScenarioDefinition) []mergedScenario {
	out := make([]mergedScenario, 0, len(progressScenarios))
	appendScenario := func(def progressScenarioDefinition, categoryName, subcategoryName string) {
		thresholds := make([]float64, len(def.RankThresholds))
		copy(thresholds, def.RankThresholds)

		out = append(out, mergedScenario{
			Name:            strings.TrimSpace(def.Name),
			CategoryName:    strings.TrimSpace(categoryName),
			SubcategoryName: strings.TrimSpace(subcategoryName),
			SortOrder:       len(out),
			RankThresholds:  thresholds,
		})
	}

	if len(difficulty.Categories) == 0 {
		for _, def := range progressScenarios {
			appendScenario(def, "", "")
		}
		return out
	}

	position := 0
	for _, category := range difficulty.Categories {
		for _, subcategory := range category.Subcategories {
			take := subcategory.ScenarioCount
			if take < 0 {
				take = 0
			}

			end := position + take
			if end > len(progressScenarios) {
				end = len(progressScenarios)
			}

			for _, def := range progressScenarios[position:end] {
				appendScenario(def, category.CategoryName, subcategory.SubcategoryName)
			}

			position = end
		}
	}

	if position < len(progressScenarios) {
		lastCategory := difficulty.Categories[len(difficulty.Categories)-1]
		for _, def := range progressScenarios[position:] {
			// Mirrors app grouping fallback: leftovers go in the final category unnamed group.
			appendScenario(def, lastCategory.CategoryName, "")
		}
	}

	return out
}

func collectScenarioLinks(difficulty sourceDifficulty) []difficultyScenarioLink {
	out := make([]difficultyScenarioLink, 0, len(difficulty.MergedScenarios))
	seen := make(map[string]struct{})

	for _, merged := range difficulty.MergedScenarios {
		name := strings.TrimSpace(merged.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		rankThresholds := make([]float64, len(merged.RankThresholds))
		copy(rankThresholds, merged.RankThresholds)

		out = append(out, difficultyScenarioLink{
			Name:            name,
			CategoryName:    strings.TrimSpace(merged.CategoryName),
			SubcategoryName: strings.TrimSpace(merged.SubcategoryName),
			SortOrder:       len(out),
			RankThresholds:  rankThresholds,
		})
	}

	return out
}
