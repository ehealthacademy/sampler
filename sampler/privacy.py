from collections.abc import Iterable
from uuid import uuid4

import pandas as pd


def generate_anonymized_id_mapping(ids: Iterable[str]) -> dict[str, str]:
    mapping = {v: str(uuid4()) for v in ids}

    # ensure no collisions
    if len(set(mapping.values())) != len(mapping.values()):
        err_msg = "Collision in generated ids"
        raise ValueError(err_msg)

    return mapping


def anonymize_dataset(
    df: pd.DataFrame,
    professional_id_mapping: dict[str, str],
    organization_id_mapping: dict[str, str],
) -> pd.DataFrame:
    professionals = set(df["professional_id"].to_list())
    organizations = set(df["organization_id"].to_list())

    df = df.replace(
        {
            "professional_id": professional_id_mapping,
            "organization_id": organization_id_mapping,
        }
    )

    res = df.replace(
        {
            "professional_id": professional_id_mapping,
            "organization_id": organization_id_mapping,
        }
    )

    if res["professional_id"].isin(professionals).any():
        err_msg = "Some original professional ids were found in the mapping"
        raise ValueError(err_msg)

    if res["organization_id"].isin(organizations).any():
        err_msg = "Some original organization ids were found in the mapping"
        raise ValueError(err_msg)

    return res
