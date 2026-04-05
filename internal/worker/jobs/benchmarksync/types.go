package benchmarksync

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
	RankColors         []sourceRankColor `json:"-"`
	Categories         []sourceCategory  `json:"categories"`
	MergedScenarios    []mergedScenario  `json:"-"`
	MergedRanks        []mergedRank      `json:"-"`
}

type sourceRankColor struct {
	Name  string
	Color string
}

type sourceCategory struct {
	CategoryName  string              `json:"categoryName"`
	Color         string              `json:"color"`
	Subcategories []sourceSubcategory `json:"subcategories"`
}

type sourceSubcategory struct {
	SubcategoryName string `json:"subcategoryName"`
	ScenarioCount   int    `json:"scenarioCount"`
	Color           string `json:"color"`
}

type mergedScenario struct {
	Name            string
	CategoryName    string
	SubcategoryName string
	SortOrder       int
	RankThresholds  []float64
}

type mergedRank struct {
	Name      string
	Color     string
	SortOrder int
}

type difficultyScenarioLink struct {
	Name            string
	CategoryName    string
	SubcategoryName string
	SortOrder       int
	RankThresholds  []float64
}

type progressScenarioDefinition struct {
	Name           string
	RankThresholds []float64
}
