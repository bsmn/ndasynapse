# Syncing BSMN data at NDA in Synapse

Uses the [NIMH Data Archive Web Services](https://data-archive.nimh.nih.gov/API) to synchronize files and metadata (annotations) stored in an NDA-hosted Amazon S3 bucket to a [Synapse Project](https://www.synapse.org/bsmn_private).

See this [IPython Notebook](docs/NDA_Data_from_GUID_API.ipynb) for examples.

## Installation

```
pip install git+https://github.com/bsmn/ndasynapse.git
```

## Docker

```
docker pull bsmn/ndasynapse
```

## Usage

The main tool to use is `query-nda`, which is installed as a command line tool when installing the package.

``` shell
> query-nda -h
usage: query-nda [-h] [--verbose] [--config CONFIG]
                 {get-experiments,get-submissions,get-submission,get-collection-manifests}
                 ...

positional arguments:
  {get-experiments,get-submissions,get-submission,get-collection-manifests}
                        sub-command help
    get-experiments     Get experiments from NDA.
    get-submissions     Get submissions in NDA collections.
    get-submission      Get an NDA submission.
    get-collection-manifests
                        Get an NDA submission.

optional arguments:
  -h, --help            show this help message and exit
  --verbose
  --config CONFIG

```
