# Setting up the SGN Development Environment

This document describes how to set up a development environment for the SGN
family of libraries. The SGN family of libraries includes:

- [`sgn`](https://greg.docs.ligo.org/sgn/): Base library for SGN
- [`sgn-ts`](https://greg.docs.ligo.org/sgn-ts): TimeSeries utilities for SGN
- [`sgn-ligo`](https://greg.docs.ligo.org/sgn-ligo): LSC specific utilities for SGN
- [`sgnl`](https://greg.docs.ligo.org/sgnl): SGN inspiraL

## Overview

The current dev environment is setup in 3 steps:

1. Clone the repos
2. Create a new conda environment
3. Install the packages

## Clone repos

The repos can be cloned using the below commands. It is recommended to clone
all the repos to have the complete set of tools available, in the same
directory.

```bash
git clone git@git.ligo.org:greg/sgn.git
git clone git@git.ligo.org:greg/sgn-ts.git
git clone git@git.ligo.org:greg/sgn-ligo.git
git clone git@git.ligo.org:greg/sgnl.git
```

Optional dependencies:

```bash
git clone git@git.ligo.org:greg/strike.git
git clone git@git.ligo.org:greg/stillsuit.git
```

## Prepare Isolated Python Environment

Prepare a new python environment via your preferred method. Below is an example
using conda.

```bash
conda create -n sgn-env python=3.10
conda activate sgn-env
```

### Install External Dependencies

For the full `sgnl` experience, install the following packages:

- numpy, pandas, scipy, matplotlib
- torch (pytorch), pytest
- confluent-kafka "python-confluent-kafka"
- lalsuite, ligo-scald, python-ligo-lw

FYI, `conda` seems to have fewer problems than `pip` at the moment.

```bash
# Basic installs
conda install -c conda-forge numpy, pandas, scipy, matplotlib, pytorch pytest

# Service installs
conda install -c conda-forge python-confluent-kafka

# LIGO installs
conda install -c lscsoft lalsuite ligo-scald python-ligo-lw
```

### Install SGN Family of Libraries

Install the SGN family of libraries using the below command in each
respective directory, in the order listed below.

```bash
pip install -e .
```

- `sgn`
- `sgn-ts`
- `sgn-ligo`
- `sgnl`

## Test the Installation

To test the installation, run the below command in the `sgnl/tests` directory.

```bash
pytest
```

This will run the whitenoise pipeline test.
