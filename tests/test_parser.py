import pytest

from sampler.parser import (
    WrongColumnNameError,
    WrongColumnTypeError,
    parse_events,
)


class TestLogDataParser:
    def test_parses_without_raising(self) -> None:
        parse_events("./tests/data/example_data_20240306.csv")
        assert True

    def test_raises_on_invalid_file(self) -> None:
        with pytest.raises(FileNotFoundError):
            parse_events("invalid_file.csv")

    def test_raises_on_invalid_column_name(self) -> None:
        with pytest.raises(WrongColumnNameError) as e:
            parse_events("./tests/data/wrong_column_name.csv")

        assert e.value.missing_columns == {"professional_cohort"}
        assert e.value.extra_columns == {"professional_cohortes"}

    def test_raises_on_invalid_column_type_cohort(self) -> None:
        with pytest.raises(WrongColumnTypeError) as e:
            parse_events("./tests/data/wrong_column_type_cohort.csv")

        assert e.value.column == "professional_cohort"

    def test_raises_on_invalid_column_type_ts(self) -> None:
        with pytest.raises(WrongColumnTypeError) as e:
            parse_events("./tests/data/wrong_column_type_ts.csv")

        assert e.value.column == "ts"
