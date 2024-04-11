import numpy as np
import pandas as pd


def compare_average_history_length(original: pd.DataFrame, sampled: pd.DataFrame):
    original_agg = original.groupby(["organization_id", "professional_id", "professional_cohort"]).size().mean()
    sampled_agg = sampled.groupby(["organization_id", "professional_id", "professional_cohort"]).size().mean()

    print(f"Original: {original_agg}")
    print(f"Sampled: {sampled_agg}")
    print(f"Diff: {sampled_agg - original_agg}")
    return sampled_agg - original_agg


def compare_percentiles_history_length(original: pd.DataFrame, sampled: pd.DataFrame):
    original_agg = (
        original.groupby(["organization_id", "professional_id", "professional_cohort"])
        .size()
        .reset_index(name="history_length")
    )
    sampled_agg = (
        sampled.groupby(["organization_id", "professional_id", "professional_cohort"])
        .size()
        .reset_index(name="history_length")
    )

    percentiles = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    original_percentiles = np.percentile(original_agg["history_length"], percentiles)
    sampled_percentiles = np.percentile(sampled_agg["history_length"], percentiles)
    comparison_df = pd.DataFrame(
        {
            "Percentile": percentiles,
            "Full Data": original_percentiles,
            "Sample": sampled_percentiles,
            "Difference": np.abs(sampled_percentiles - original_percentiles),
        }
    )

    print(comparison_df)
