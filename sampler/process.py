import json
import math
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Optional

import pandas as pd

from sampler.config import Mappings
from sampler.privacy import anonymize_dataset, generate_anonymized_id_mapping


def filter_events(
    events: pd.DataFrame,
    after: date,
    until: date,
    excluded_ids: Iterable[str] = [],
) -> pd.DataFrame:
    df = events[
        (events["ts"] >= pd.Timestamp(after))
        & (events["ts"] <= pd.Timestamp(until))
        & (~events["professional_id"].isin(excluded_ids))
    ]
    return df


def history_length_per_professional(events: pd.DataFrame) -> pd.DataFrame:
    df_agg = events.groupby(["organization_id", "professional_id", "professional_cohort"]).size()
    return df_agg.reset_index(name="history_length")


def calculate_bucket_size(events: pd.DataFrame) -> pd.DataFrame:
    df_agg = events.groupby(["organization_id", "professional_cohort"]).agg(
        {"history_length": ["size", "mean"]},
        as_index=False,
    )
    df_agg.columns = ["bucket_size", "avg_history_length"]
    return df_agg.reset_index()


def assign_sample_count(buckets: pd.DataFrame, sample_size: int) -> pd.DataFrame:
    total = buckets["bucket_size"].sum()
    if sample_size >= total:
        return buckets

    buckets["proportion"] = buckets["bucket_size"] / total
    buckets["proportional_expected_samples"] = buckets["proportion"] * sample_size

    buckets = buckets.sort_values(by="proportional_expected_samples", ascending=False)
    buckets["expected_samples"] = buckets["proportional_expected_samples"].apply(lambda x: math.ceil(x)).astype(int)

    buckets["expected_samples"] = buckets["expected_samples"].apply(lambda x: x if x > 0 else 1)

    # for the ones with the same value, sort by average history length descending
    buckets = buckets.sort_values(by=["expected_samples", "avg_history_length"], ascending=False)

    buckets["cumulative_samples"] = buckets["expected_samples"].cumsum()

    mask_under_threshold = buckets["cumulative_samples"] < sample_size

    first_over_threshold = mask_under_threshold.sum()

    filtered_df = buckets.loc[
        mask_under_threshold,
        ["organization_id", "professional_cohort", "expected_samples"],
    ]

    last_entry = buckets.iloc[first_over_threshold : first_over_threshold + 1, :].copy()
    last_entry["expected_samples"] = sample_size - (last_entry["cumulative_samples"] - last_entry["expected_samples"])

    # concatenate to the filtered_df the row on index first_over_threshold
    filtered_df = pd.concat(
        [
            filtered_df,
            last_entry.loc[:, ["organization_id", "professional_cohort", "expected_samples"]],
        ]
    )

    if filtered_df["expected_samples"].sum() != sample_size:
        err_msg = "The sum of the expected samples is not equal to the sample size"
        raise ValueError(err_msg)

    return filtered_df


def sample_from_bucket(history: pd.DataFrame, bucket: dict[str, str], sample_count: int) -> list[str]:
    # Will return the professional_ids sampled from the bucket

    condition = True
    for column, value in bucket.items():
        condition = condition & (history[column] == value)

    bucket_data = history[condition]
    sample = bucket_data.sample(n=sample_count)  # TODO: do something smarter here?

    if len(sample) != sample_count:
        err_msg = "The number of sampled professionals is not equal to the expected samples"
        raise ValueError(err_msg)

    return sample["professional_id"].tolist()


def sample_professionals(history: pd.DataFrame, sampled_buckets: pd.DataFrame) -> list[str]:
    sampled_professionals = []
    for row in sampled_buckets.itertuples():
        bucket_definition = {
            "organization_id": row.organization_id,
            "professional_cohort": row.professional_cohort,
        }
        professionals = sample_from_bucket(history, bucket_definition, row.expected_samples)

        sampled_professionals.extend(professionals)

    return sampled_professionals


@dataclass
class Result:
    sampled_professionals: list[str]
    id_mappings: Mappings
    samples: pd.DataFrame
    configuration: dict = None

    def save_to(self, output_dir: str):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.id_mappings.save_to_file(f"{output_dir}/id_mappings.json")

        pd.DataFrame(self.sampled_professionals, columns=["professional_id"]).to_csv(
            f"{output_dir}/sampled_professionals.csv", index=False
        )

        self.samples.to_csv(f"{output_dir}/sampled_anonymized_dataset.csv", index=False)

        if self.configuration:
            with open(f"{output_dir}/arguments_to_sampler_function.json", "w") as file:
                json.dump(self.configuration, file, indent=4)


def sample(
    df: pd.DataFrame,
    output_sample_count: int,
    after: Optional[date] = None,
    until: Optional[date] = None,
    excluded_ids: Iterable[str] = [],
    mappings: Optional[Mappings] = None,
    include_all_in_output: bool = False,
) -> Result:
    if not after:
        after = date(2022, 1, 1)

    if not until:
        until = date.today()

    if not mappings:
        mappings = Mappings({}, {})

    ids_excluded_for_sampling = set(excluded_ids).union(set(mappings.professionals.keys()))
    filtered_for_sampling = filter_events(df, after, until, ids_excluded_for_sampling)
    history = history_length_per_professional(filtered_for_sampling)
    buckets = calculate_bucket_size(history)
    sampled_buckets = assign_sample_count(buckets, output_sample_count)

    sampled_professionals = sample_professionals(history, sampled_buckets)

    if len(set(sampled_professionals) & set(ids_excluded_for_sampling)) != 0:
        err_msg = "Some sampled professionals were found in the excluded ids"
        raise ValueError(err_msg)

    professionals_in_output = sampled_professionals.copy()
    if include_all_in_output:
        professionals_in_output.extend(set(mappings.professionals.keys()))

        # if something is in the excluded_ids, we remove it
        professionals_in_output = set(professionals_in_output) - set(excluded_ids)

    # filter again without the mapped ids
    filtered = filter_events(df, after, until, excluded_ids)
    selected_entries = filtered[filtered["professional_id"].isin(professionals_in_output)]
    organization_ids = set(selected_entries["organization_id"].to_list())

    missing_organizations = organization_ids - set(mappings.organizations.keys())
    organizations_mappings = {
        **generate_anonymized_id_mapping(missing_organizations),
        **mappings.organizations,
    }

    missing_professionals = set(sampled_professionals) - set(mappings.professionals.keys())
    professionals_mappings = {
        **generate_anonymized_id_mapping(missing_professionals),
        **mappings.professionals,
    }

    anonymized_selected_entries = anonymize_dataset(selected_entries, professionals_mappings, organizations_mappings)

    return Result(
        sampled_professionals=sampled_professionals,
        samples=anonymized_selected_entries,
        id_mappings=Mappings(
            organizations=organizations_mappings,
            professionals=professionals_mappings,
        ),
        configuration=(
            {
                "after": after.isoformat(),
                "until": until.isoformat(),
                "excluded_ids": excluded_ids,
                "output_sample_count": output_sample_count,
                "include_all_in_output": include_all_in_output,
                "input_mappings": mappings.to_dict(),
            }
        ),
    )
