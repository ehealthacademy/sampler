import json
from dataclasses import dataclass
from datetime import date


@dataclass
class Config:
    number_of_samples: int
    start_period: date
    end_period: date


def read_config(filename: str) -> Config:
    with open(filename) as file:
        config_data = json.load(file)

    return Config(
        number_of_samples=config_data["number_of_samples"],
        start_period=date.fromisoformat(config_data["start_period"]),
        end_period=date.fromisoformat(config_data["end_period"]),
    )


@dataclass
class Mappings:
    organizations: dict[str, str]
    professionals: dict[str, str]

    def to_dict(self) -> dict:
        return {
            "organizations": self.organizations,
            "professionals": self.professionals,
        }

    @classmethod
    def load_from_file(cls, filename: str) -> "Mappings":
        with open(filename) as file:
            mappings_data = json.load(file)
        return cls(
            organizations=mappings_data["organizations"],
            professionals=mappings_data["professionals"],
        )

    def save_to_file(self, filename: str) -> None:
        mappings_data = {
            "organizations": self.organizations,
            "professionals": self.professionals,
        }
        with open(filename, "w") as file:
            json.dump(mappings_data, file, indent=4)
