# A client for querying NDA and synchronizing to Synapse

This package provides an interface to the [NIMH Data Archive Web Services](https://data-archive.nimh.nih.gov/API). It can be used to query data in NDA as well as synchronize files and metadata (annotations) stored in an NDA-hosted Amazon S3 bucket to a [Synapse Project](https://www.synapse.org/bsmn_private).

## Installation

```
pip install git+https://github.com/bsmn/ndasynapse.git
```

Since release 1.0.0, this package is available on PyPi:

```
pip install ndasynapse
```

## Configuration

Authentication to NDA requires a JSON configuration file, that should look like this:

```
{
  "nda": {"password": "yourpassword",
          "username": "yourusername",
          "submission.service.url": "https://nda.nih.gov/api/submission",
          "guid.service.url": "https://nda.nih.gov/api/guid",
          "experiment.service.url": "https://nda.nih.gov/api/experiment"}
}
```

## Usage

The main tool to use is `query-nda`, which is installed as a command line tool when installing the package. Use the command `query-nda -h` to learn about the features it offers, including sub-commands and arguments.

## Contributing

### Fork and clone this repository

See the [Github docs](https://help.github.com/articles/fork-a-repo/) for how to make a copy (a fork) of a repository to your own Github account.

Then, [clone the repository](https://help.github.com/articles/cloning-a-repository/) to your local machine so you can begin making changes.

Add this repository as an [upstream remote](https://help.github.com/en/articles/configuring-a-remote-for-a-fork) on your local git repository so that you are able to fetch the latest commits.

On your local machine make sure you have the latest version of the `develop` branch:

```
git checkout develop
git pull upstream develop
```

### The development life cycle

1. Pull the latest content from the `develop` branch of this central repository (not your fork).
1. Create a feature branch which off the `develop` branch. If there is a GitHub issue that you are addressing, name the branch after the issue with some more detail (like `issue-123-add-some-new-feature`).
1. After completing work and testing locally (see below), push to your fork.
1. In Github, create a pull request from the feature branch of your fork to the `develop` branch of the central repository.

> *A code maintainer must review and accept your pull request.* A code review (which happens with both the contributor and the reviewer present) is required for contributing. This can be performed remotely (e.g., Skype, Hangout, or other video or phone conference).

### Releases

This package uses semantic versioning for releasing new versions. The version should be updated on the `develop` branch as changes are reviewed and merged in by a code maintainer. The version for the package is maintained in the [ndasynapse/__version__.py](ndasynapse/__version__.py) file.

A [GitHub Action workflow](.github/workflows/pythonpublish.yml) pushes GitHub release tags to [PyPi](https://pypi.org/project/ndasynapse/). Create a new GitHub release and the action will trigger. 

### Testing

Please strongly consider adding tests for new code. These might include unit tests (to test specific functionality of code that was added to support fixing the bug or feature), integration tests (to test that the feature is usable - e.g., it should have complete the expected behavior as reported in the feature request or bug report), or both.

This package uses [`nose`](http://nose.readthedocs.io/) to run tests. The test code is located in the [test](./test) subdirectory.

Here's how to run the test suite:

```
nosetests -vs tests/
```
