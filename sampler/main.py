import argparse
import os
from datetime import date

from sampler.config import Mappings, read_config
from sampler.metrics import (
    compare_average_history_length,
    compare_percentiles_history_length,
)
from sampler.parser import parse_events
from sampler.process import sample

if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to the dataset CSV file")
    parser.add_argument("--config", help="The extractor configuration file", required=True)
    parser.add_argument("--id_mappings", help="Path to the id_mappings JSON file")
    parser.add_argument(
        "--include_all_in_output",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include all professionals in the output (sampled and excluded)",
    )
    args = parser.parse_args()

    config = read_config(args.config)

    sampler_args = {
        "after": config.start_period,
        "until": config.end_period,
        "include_all_in_output": args.include_all_in_output,
    }

    if args.id_mappings:
        id_mappings = Mappings.load_from_file(args.id_mappings)
        sampler_args = {
            **sampler_args,
            "mappings": id_mappings,
            "excluded_ids": id_mappings.professionals.keys(),
        }

    df = parse_events(args.input)
    result = sample(df, config.number_of_samples, **sampler_args)

    compare_average_history_length(df, result.samples)
    compare_percentiles_history_length(df, result.samples)

    output_dir = f"out/{date.today()}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    result.save_to(output_dir)
