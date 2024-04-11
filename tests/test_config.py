from datetime import date

from sampler.config import Mappings, read_config


def test_config_parser():
    config_filename = "./tests/data/config.json"

    config = read_config(config_filename)

    assert config.number_of_samples == 300
    assert config.start_period == date(2022, 1, 1)
    assert config.end_period == date(2024, 4, 1)


def test_mappings_parser():
    mappings_filename = "./tests/data/id_mappings.json"

    mappings = Mappings.load_from_file(mappings_filename)

    assert mappings.organizations == {
        "org1": "f02519c7-4ff5-47b7-b743-56461a7288df",
        "org2": "5f4aa9e9-ef63-407e-90b2-0a56c1862c54",
    }
    assert mappings.professionals == {
        "prof1": "d5eeb5e3-8d64-46ad-987d-64900ae2cd48",
        "prof2": "10952ac2-b14f-4b8b-b0f6-55e7b4f701a8",
    }
