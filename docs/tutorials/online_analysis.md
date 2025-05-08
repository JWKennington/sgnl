# Setting up the SGNL online analysis

This document describes how to set up an online CBC analysis with SGNL.

## Prepare container

Follow the [installation guide](../install.md) and make a **singularity container**.

### Install pastro

To run an online analysis with the `sgnl-ll-pastro-uploader`, the `pastro` repo needs
to be installed. A common practice is to install git repos under `CONTAINER_NAME/src`.

```bash
git clone -b sgnl git@git.ligo.org:gstlal/pastro.git
cd pastro
pip install -e .
```

### Build sif container

```bash
singularity build CONTAINER_NAME.sif CONTAINER_NAME
```

## Prepare working directory

In your working directory, copy over the following files from the repo

1. `config/online_dag.yml`:

    This is the config file for generating the online analysis workflow.
    Modify the config file options to setup the configuration. Note that
    the `container:` field shoud be the `sif` container.

2. `config/cbc_db.yaml`:

    This is the config file for creating trigger databases.

## Create Workflow

Workflows can be created by:

```bash
singularity exec CONTAINER_NAME sgnl-ll-dagger -c <online config file> -w <workflow>
``` 

Currently the supported online workflows are:

1. `setup`
    - setup pre-generated files
2. `inspiral`
    - the online analysis

## Launch workflow

After creating a workflow, launch the dag:

```bash
condor_submit_dag <dag_name>.dag
```
