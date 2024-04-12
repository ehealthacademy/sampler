from datetime import date

import numpy as np
import pandas as pd
import pytest

from sampler.parser import parse_events
from sampler.process import (
    assign_sample_count,
    calculate_bucket_size,
    filter_events,
    history_length_per_professional,
    sample,
    sample_from_bucket,
    sample_professionals,
)
from sampler.utils import bucketize_cohort


@pytest.fixture
def dataset() -> pd.DataFrame:
    return parse_events("./tests/data/example_data_20240306.csv")


class TestFilter:
    def test_filters_by_date(self, dataset: pd.DataFrame) -> None:
        filtered = filter_events(dataset, date(2023, 4, 3), date(2023, 4, 6))
        assert len(filtered) == 28

        assert (filtered["ts"] >= pd.Timestamp(date(2023, 4, 3))).all()
        assert (filtered["ts"] <= pd.Timestamp(date(2023, 4, 6))).all()

    def test_filters_by_date_and_excluded_ids(self, dataset: pd.DataFrame) -> None:
        filtered = filter_events(
            dataset,
            date(2023, 4, 3),
            date(2023, 4, 6),
            [
                "8e129b1d-43bc-4d70-ad2f-dd924c0b3b03",
                "a7ca0a93-e198-4300-ba0b-d77cf6716827",
            ],
        )
        assert len(filtered) == 15

        assert (filtered["ts"] >= pd.Timestamp(date(2023, 4, 3))).all()
        assert (filtered["ts"] <= pd.Timestamp(date(2023, 4, 6))).all()

        assert not (
            filtered["professional_id"].isin(
                [
                    "8e129b1d-43bc-4d70-ad2f-dd924c0b3b03",
                    "a7ca0a93-e198-4300-ba0b-d77cf6716827",
                ]
            )
        ).all()


class TestProcessing:
    def test_history_length_per_professional(self, dataset: pd.DataFrame) -> None:
        df = history_length_per_professional(dataset)
        df_agg = df[df["professional_id"] == "10952ac2-b14f-4b8b-b0f6-55e7b4f701a8"]

        assert len(df_agg) == 1
        assert (df_agg["history_length"] == 608).all()

    def test_calculate_bucket_size(self, dataset: pd.DataFrame) -> None:
        df = history_length_per_professional(dataset)
        buckets = calculate_bucket_size(df)
        selected = buckets[
            (buckets["professional_cohort"] == bucketize_cohort("2019-01-01"))
            & (buckets["organization_id"] == "f02519c7-4ff5-47b7-b743-56461a7288df")
        ]

        assert len(selected) == 1
        assert (selected["bucket_size"] == 10).all()

    def test_assign_sample_count(self, dataset: pd.DataFrame) -> None:
        total = 50
        df = history_length_per_professional(dataset)
        buckets = calculate_bucket_size(df)
        buckets = assign_sample_count(buckets, total)
        assert len(buckets) == 47
        assert buckets["expected_samples"].sum() == total

    def test_sample_from_bucket(self, dataset: pd.DataFrame) -> None:
        total = 50
        df = history_length_per_professional(dataset)
        buckets = calculate_bucket_size(df)
        buckets = assign_sample_count(buckets, total)

        top_bucket = buckets.iloc[0]

        np.random.seed(1)  # to make the test deterministic
        expected_samples = top_bucket["expected_samples"]
        sampled = sample_from_bucket(
            df,
            {
                "organization_id": top_bucket["organization_id"],
                "professional_cohort": top_bucket["professional_cohort"],
            },
            expected_samples,
        )

        assert len(sampled) == expected_samples

    def test_sample_professionals(self, dataset: pd.DataFrame) -> None:
        total = 3
        df = history_length_per_professional(dataset)
        buckets = calculate_bucket_size(df)
        buckets = assign_sample_count(buckets, total)

        np.random.seed(1)  # to make the test deterministic
        professionals = sample_professionals(df, buckets)

        print(professionals)
        assert len(professionals) == total


class TestSampler:
    def test_sample(self, dataset: pd.DataFrame) -> None:
        sampled = sample(dataset, 30, excluded_ids=["8e129b1d-43bc-4d70-ad2f-dd924c0b3b03"])

        assert len(sampled.sampled_professionals) == 30
        assert len(set(sampled.samples["professional_id"].to_list())) == 30

        sampled_with_exclusions = sample(
            dataset,
            30,
            mappings=sampled.id_mappings,
            excluded_ids=sampled.sampled_professionals,
        )

        assert len(sampled_with_exclusions.sampled_professionals) == 30
        assert len(set(sampled_with_exclusions.samples["professional_id"].to_list())) == 30

        sampled_with_exclusions_include_all = sample(
            dataset,
            30,
            mappings=sampled.id_mappings,
            excluded_ids=sampled.sampled_professionals,
            include_all_in_output=True,
        )

        assert len(sampled_with_exclusions_include_all.sampled_professionals) == 30
        assert len(set(sampled_with_exclusions_include_all.samples["professional_id"].to_list())) == 30

        sampled_with_exclusions_include_all = sample(
            dataset,
            30,
            mappings=sampled.id_mappings,
            include_all_in_output=True,
        )

        assert len(sampled_with_exclusions.sampled_professionals) == 30
        assert len(set(sampled_with_exclusions_include_all.samples["professional_id"].to_list())) == 60
