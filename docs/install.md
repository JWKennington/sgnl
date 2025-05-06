# Setting up the SGN Development Environment

This document describes how to set up a development environment for the SGN
family of libraries. The SGN family of libraries includes:

- [`sgn`](https://greg.docs.ligo.org/sgn/): Base library for SGN
- [`sgn-ts`](https://greg.docs.ligo.org/sgn-ts): TimeSeries utilities for SGN
- [`sgn-ligo`](https://greg.docs.ligo.org/sgn-ligo): LSC specific utilities for SGN
- [`sgnl`](https://greg.docs.ligo.org/sgnl): SGN inspiraL

## Overview

The current dev environment is setup in 3 steps:

1. (Optional) Prepare an environment with one of the following options:
    - Writable singularity container
    - Conda environment
2. Clone the repos
3. Install the packages

## Prepare Environment

### Prepare a Writable Singularity Container

```bash
singularity build --fix-perms --sandbox CONTAINER_NAME docker://containers.ligo.org/greg/sgnl:latest
singularity run --writable CONTAINER_NAME
```

### Prepare Isolated Python Environment

Prepare a new python environment via your preferred method. Below is an example
using conda.

```bash
conda create -n sgn-env python=3.10
conda activate sgn-env
```

## Clone repos

The repos can be cloned using the below commands. It is recommended to clone
all the repos to have the complete set of tools available, in the same
directory. If you are using a singularity container, a common pratice is to
clone the repos under `CONTAINER_NAME/src`.

### Note: Git LFS and Test Data

The `sgnl` library uses git-lfs to store test data. To download the test data
files, install `git-lfs` and run `git lfs install`. If you have already cloned
the repos, you can run `git lfs pull` in the respective directories to download
the test data.

### Clone the Repos in Order

Clone the repos in the order listed below. The first two are only required for
the `sgnl` library.

```bash
# First two only required for SGNL
git clone git@git.ligo.org:greg/strike.git
git clone git@git.ligo.org:greg/stillsuit.git

git clone git@git.ligo.org:greg/sgn.git
git clone git@git.ligo.org:greg/sgn-ts.git
git clone git@git.ligo.org:greg/sgn-ligo.git
git clone git@git.ligo.org:greg/sgnl.git
```

## Install SGN Family of Libraries

Install the SGN family of libraries using the below command in each
respective directory, in the order listed below. To get all development
dependencies, use the `[dev]` tag on the sgn\* packages when using the local
editable pip install `pip install -e .`. Specific commands
are given below, that assume all repositories have been cloned into a common
parent directory, which is the cwd at the beginning of the below.

```bash
# Install stillsuit
cd stillsuit
pip install -e .

# Install strike
cd ../strike
pip install -e .

# Install sgn
cd ../sgn
pip install -e .[dev]

# Install sgn-ts
cd ../sgn-ts
pip install -e .[dev]

# Install sgn-ligo
cd ../sgn-ligo
pip install -e .[dev]

# Install sgnl
cd ../sgnl
pip install -e .[dev]
```

As reflected above, the general order of install should be the inverse depth-sorted order of
package dependencies. The order is:

- `stillsuit`
- `strike`
- `sgn`
- `sgn-ts`
- `sgn-ligo`
- `sgnl`

### Install strike data

If you are not using a singularity container, you will need to run the following to
install pre-generated strike data.

```bash
strike-config set --path <path> --setup
export STRIKE_DATA_PATH=<path>
```

The default path that strike looks for is `${HOME}/strike_data`.

## Test the Installation

To test the installation, run the below command in the `sgnl/tests` directory.

```bash
cd /path/to/parent/sgnl/tests
pytest
```

This will run the whitenoise pipeline test.

## Optional: Manual Install of External Dependencies

In earlier versions of the libraries, some external dependencies were not
installed automatically via package dependencies. Though this is not required
anymore, you can manually install the below packages if needed.

The packages are listed below in terms of pip-friendly names, (conda names in parantheses).

- numpy, pandas, scipy, matplotlib
- torch (pytorch), pytest
- confluent-kafka "python-confluent-kafka"
- lalsuite, ligo-scald, python-ligo-lw

Example installs with conda:

```bash
# Basic installs
conda install -c conda-forge numpy pandas scipy matplotlib pytorch pytest

# Service installs
conda install -c conda-forge python-confluent-kafka

# LIGO installs
conda install -c conda-forge lalsuite ligo-scald python-ligo-lw
```
