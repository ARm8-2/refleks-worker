package scenariostats

import "testing"

func TestBuildMetricSummaryCapturesExactQuantilesAndOutliers(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 100}
	summary := buildMetricSummary(values)

	if summary.SampleCount != 6 {
		t.Fatalf("sample count = %d, want 6", summary.SampleCount)
	}
	if summary.Quantiles.P50 == nil || *summary.Quantiles.P50 != 3.5 {
		t.Fatalf("median = %v, want 3.5", summary.Quantiles.P50)
	}
	if summary.Quantiles.P95 == nil || *summary.Quantiles.P95 != 76.25 {
		t.Fatalf("p95 = %v, want 76.25", summary.Quantiles.P95)
	}
	if summary.Outliers.UpperCount != 1 {
		t.Fatalf("upper outlier count = %d, want 1", summary.Outliers.UpperCount)
	}
	if summary.Histogram.BinCount != histogramBinCount {
		t.Fatalf("histogram bin count = %d, want %d", summary.Histogram.BinCount, histogramBinCount)
	}
	if summary.Quantiles.P99 == nil || *summary.Quantiles.P99 <= *summary.Quantiles.P95 {
		t.Fatalf("p99 = %v, want greater than p95 %v", summary.Quantiles.P99, summary.Quantiles.P95)
	}
}

func TestBuildMetricSummaryHandlesSingleValue(t *testing.T) {
	summary := buildMetricSummary([]float64{42})

	if summary.Histogram.BinCount != 1 {
		t.Fatalf("histogram bin count = %d, want 1", summary.Histogram.BinCount)
	}
	if len(summary.Histogram.Bins) != 1 {
		t.Fatalf("histogram bins = %d, want 1", len(summary.Histogram.Bins))
	}
	if summary.Histogram.Bins[0].Count != 1 {
		t.Fatalf("single bin count = %d, want 1", summary.Histogram.Bins[0].Count)
	}
	if summary.Quantiles.P50 == nil || *summary.Quantiles.P50 != 42 {
		t.Fatalf("median = %v, want 42", summary.Quantiles.P50)
	}
}
