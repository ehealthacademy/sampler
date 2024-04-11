import re
from collections.abc import Iterable
from datetime import datetime
from functools import partial

import pandas as pd

from sampler.utils import bucketize_cohort


class WrongColumnNameError(Exception):
    missing_columns: set[str]
    extra_columns: set[str]

    def __init__(self, expected_columns: Iterable[str], got_columns: Iterable[str]):
        self.missing_columns = set(expected_columns) - set(got_columns)
        self.extra_columns = set(got_columns) - set(expected_columns)

        super().__init__(
            f"Missing columns: {', '.join(self.missing_columns)}, Extra columns: {', '.join(self.extra_columns)}"
        )


class WrongColumnTypeError(Exception):
    column: str
    error: Exception

    def __init__(self, column, error: Exception):
        super().__init__(f"Wrong type in column: {column}: {error}")
        self.column = column
        self.error = error


def as_str(col_name, col):
    if not isinstance(col, str):
        raise WrongColumnTypeError(col_name, ValueError(f"Expected string, got {col}"))
    return col


def as_yyyymmdd(col_name, col):
    if not isinstance(col, str):
        raise WrongColumnTypeError(col_name, ValueError(f"Expected string, got {col}"))
    if not re.match(r"\d{4}-\d{2}-\d{2}", col):
        raise WrongColumnTypeError(col_name, ValueError(f"Expected YYYY-MM-DD, got {col}"))
    return bucketize_cohort(col)


def as_iso_timestamp(col_name, col) -> pd.Timestamp:
    if not isinstance(col, str):
        raise WrongColumnTypeError(col_name, ValueError(f"Expected string, got {col}"))
    if not re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", col):
        raise WrongColumnTypeError(
            col_name,
            ValueError(f"Expected ISO timestamp YYYY-MM-DDThh:mm:ss, got {col}"),
        )

    try:
        col = datetime.strptime(col, "%Y-%m-%dT%H:%M:%S")
        return pd.Timestamp(col)
    except ValueError as e:
        raise WrongColumnTypeError(col_name, ValueError(f"Invalid ISO timestamp format: {col}")) from e


def parse_events(filename: str) -> pd.DataFrame:
    columns = [
        "organization_id",
        "professional_id",
        "professional_cohort",
        "ts",
        "event_type",
    ]

    df = pd.read_csv(
        filename,
        converters={
            "organization_id": partial(as_str, "organization_id"),
            "professional_id": partial(as_str, "professional_id"),
            "professional_cohort": partial(as_yyyymmdd, "professional_cohort"),
            "ts": partial(as_iso_timestamp, "ts"),
            "event_type": partial(as_str, "event_type"),
        },
    )

    if not all(col in df.columns for col in columns):
        raise WrongColumnNameError(columns, df.columns)

    return df
