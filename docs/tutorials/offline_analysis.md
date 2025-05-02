# Setting up the SGNL offline analysis

This document describes how to set up an offline CBC analysis with SGNL.

## Prepare conda env

Follow the [installation guide](../install.md) and make a singularity container.

## Prepare working directory

In your working directory, copy over the following files from the repo

1. `config/offline_dag.yml`:

    This is the config file for generating the offline analysis workflow.
    Modify the config file options to setup the configuration.

2. `config/cbc_db.yaml`:

    This is the config file for creating trigger databases.

## Create Workflow

Workflows can be created by:

```bash
sgnl-dagger -c <offline config file> -w <workflow>
``` 

Currently the supported offline workflows are:

1. `psd`
2. `filter`
3. `injeciton-filter`
4. `rank`

## Launch workflow

After creating a workflow, launch the dag:

```bash
condor_submit_dag sgnl_<workflow>.dag
```
