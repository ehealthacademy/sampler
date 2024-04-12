# sampler

Running this script provides a uniform way to do sampling on the dataset for the ehealth academy research.

It takes as input a CSV file with the following columns:

- `organization_id`: the organization id. This is the id of the organization that the professional belongs to. It will be anonymized by the script for the final dataset.
- `professional_id`: the professional id. This is the id of the professional that the event belongs to. It will be anonymized by the script for the final dataset.
- `professional_cohort`: the year-month that the professional account was created in YYYY-MM-01 format (eg. 2020-06-01)
- `ts`: the timestamp of the event. This is the timestamp of the event in the format YYYY-MM-DDTHH:MM:SS (eg. 2020-06-01T12:00:00)
- `event_type`: One of the following values:
  `feedback_sent`,
  `client_account_created`,
  `message_sent`,
  `intervention_added`,
  `videocall_started`
  .

In the `config` directory, there are files for each of the data extractions. These files contain the following information:

- `number_of_samples`
- `start_period`
- `end_period`

## Getting started

Dependencies are managed via poetry.

You can use `make install` to install the dependencies. If you don't have `make` installed, you can run `poetry install` instead.

## Running the script

The script supports excluding professional ids from the dataset. This functionality is not meant to be used to exclude professionals for whom we have already delivered data.

`python sampler/main.py --config config/dataset_1/wave_1.json <filename with the data>.csv`

## Output

At the end of the run, the script outputs some summary metrics to the console. These are:

- Summaries of average length of event history per professional.
- Differences in percentiles of event history length per professional.

Example output:

```Original: 77.89143135345667
Sampled: 86.55666666666667
Diff: 8.665235313210005

   Percentile  Full Data  Sample  Difference
0          10        2.0     2.0         0.0
1          20        4.0     5.0         1.0
2          30        7.0     9.0         2.0
3          40       11.0    14.0         3.0
4          50       17.0    34.5        17.5
5          60       27.0    49.0        22.0
6          70       41.0    80.0        39.0
7          80       74.0   145.2        71.2
8          90      158.7   321.2       162.5
```

Additionally, the script will output a few files in a directory under `out` named with the current date. The files are:

- `id_mappings.json`: a json file with the mapping of the original professional ids to the new ones. This is to be used for subsequent data extractions and to be able to answer questions from researchers should the need arise.
- `sampled_professionals.csv`: a CSV file with the ids of the professionals that were sampled in this run of the script.
- `arguments_to_sampler_function.json`: a JSON file with the arguments that were passed to the sampler function. This is to be able to have an idea of how the sampling function was called.
- `sampled_anonymized_dataset.csv`: a CSV file with the sampled dataset. This is the file that should be shared with the researchers.

## Advanced usage

You can pass the id mappings from a previous run of the script to exclude the already sampled professionals from the sampling process. This is done by passing the `--id_mappings` argument to the script.

`python sampler/main.py --config config/dataset_1/wave_1.json --id_mappings out/2024-04-10/id_mappings.json  <filename with the data>.csv`

If you want to include in the output dataset the entries for professionals that have already been sampled before, you can pass the `--id_mappings` argument to the script with the JSON file from a previous run and add the `--include_all_in_output` flag.

`python sampler/main.py --config config/dataset_1/wave_1.json --id_mappings out/2024-04-10/id_mappings.json --include_all_in_output <filename with the data>.csv`
