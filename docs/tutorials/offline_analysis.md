# Setting up the SGNL offline analysis

This document describes how to set up an offline CBC analysis with SGNL.

## Prepare container

Follow the [installation guide](../install.md) and make a **singularity container**.

### Build sif container

```bash
singularity build CONTAINER_NAME.sif CONTAINER_NAME
```

## Prepare working directory

A working directory is where we will run the offline analysis. This directory
will contain inputs necessary for the analysis to run, and will also serve as
the output directory for the analysis. It is best to start with an empty directory
that has been made specifically for this offline run.

There are several components necessary for an offline analysis that must be
present in your working directory. Some of these components were generated
previously and will not change, such as the mass model and template bank,
while others are specific to your particular offline run, such as the PSD
and segments file.

Here's how to prepare a blank working drectory for an offline analysis:

In your working directory, copy over the following files from the `sgnl` repo

1. `config/offline_dag.yml`:

    This is the config file for generating the offline analysis workflow.
    Modify the config file options to setup the configuration. Note that
    the `container:` field shoud be the `sif` container.

2. `config/cbc_db.yaml`:

    This is the config file for creating trigger databases.

There are several lines in the config file that we need to change before launching
the offline workflows. The config file tells the analysis where to access and/or create
relevant input files like the ones we mentioned previously.

The following dataproducts must be put into the working directory:

- Template bank
- Frame cache
- Frame segments file
- SVD manifest
- Mass model

Some of these are created by the analysis itself, but there are several that aren't.

### Template bank and mass model

The template bank and mass model can be accessed in the `T2200343-v3` LIGO DCC entry.

Running the following command with an appropriate conda environment (such as `igwn`)

```bash
dcc archive --archive-dir=. --files -i --public T2200343-v3
```

will provide several files that the user can decide whether or not to download.
Note that there are several template banks to choose from. The user should select
the template bank they would like to use, as well as the `h5` file that contains
the Salpeter mass model.

Once the downloads have finished, you need to add the paths to the relevant files in
the config file.

### Frame cache

The `frame.cache` file tells the analysis where to find the relevant frame files that
contain the interferometer strain data that we hope to analyze. This file is **not**
produced by the analysis and should be made manually. 

Follow the steps outlined [here](https://gwsci.org/ops/references#make-frame-caches) to make a `frame.cache` file,
and add the path to this file in the config.

### Frame segments file

The frame segments file contains information about the state of each interferometer
over the course of the times we are interested in analyzing (whether or not the interferometer
was in observing mode, etc).

If no segments file is present in the working directory, the analysis will automatically detect this and create
one based on the start time, end time, and segments specified in the config file.


## Initialize Offline run

After gathering the **template bank, mass model, frame cache, `cbc_db.yml`** and pointing to them
in the config file, we are ready to run the initialization command.

This command uses the components we have added to the working directory to make the final
pieces we need before creating the next workflows, such as the split bank and segments files.

```bash
singularity exec CONTAINER_NAME sgnl-dagger --init -c <offline config file>
```

## Create and Launch Workflow

After the initialization command has run successfully, we are ready to create
and launch the relevant workflows for our run.

Workflows can be created by:

```bash
singularity exec CONTAINER_NAME sgnl-dagger -c <offline config file> -w <workflow>
``` 

After creating a workflow, launch the corresponding dag:

```bash
condor_submit_dag sgnl_<workflow>.dag
```

Currently the supported offline workflows are listed here
and described in more detail below:

1. `psd`
2. `svd`
3. `filter`
4. `injeciton-filter`
5. `rank`

## PSD and SVD Workflow

These workflows are a continuation of the setup process -- they produce outputs
that are used by the filter workflow to generate the CBC search outputs (triggers,
diststats, etc.).

### PSD Workflow

The PSD workflow generates Power Spectral Density estimates that characterize the noise
in each detector over the analysis period. These PSDs are used by the SVD and filter workflows
to whiten CBC templates based on actual detector noise characteristics.

The PSD workflow consists of two stages that are run in series automatically via the condor dag:

#### 1. Reference PSD Calculation

The first stage measures PSDs from the interferometer strain data for each time segment and interferometer
combination specified in the config. For each segment, the workflow:

1. **Reads frame data** from the data source specified in the config
2. **Applies conditioning** including resampling and highpas filering to the raw strain data
3. **Estimates the PSD** using the Welch method with the FFT length specified in the config
4. **Outputs individual PSD files** for each time segment and IFO combination

These reference PSDs capture the noise characteristics of the detectors during different time periods.

#### 2. Median PSD Calculation

The second stage combines all the reference PSDs to create a single representative PSD for each interferometer.
This stage:

1. **Collects all reference PSDs** produced in the first stage
2. **Computes the median** across al time segments for each frequency bin
3. **Outputs a single median PSD** for each interferometer

The median is used (rather than the mean) to reduce the impact of outlier noise transients or unusual detector
states. This median PSD represents the typical noise floor for each detector over the entire analysis period.

#### Outputs and Usage

The median PSDs are stored in the `input_data` directory and used by:

- **SVD workflow**: To whiten templates when generating SVD banks
- **Filter workflow**: To whiten the data and matched filter outputs

### SVD Workflow

The SVD (Singular Value Decomposition) workflow creates a compact representation of the template bank that
dramatically reduces computational cost during the filtering stage. This is accomplished by transforming
thousands of templates into a much smaller set of orthogonal basis vectors.

#### Prerequisite: Initialization

Before running the SVD workflow, the initialization step preforms necessary preparation steps outlined below.

#### Template Bank Splitting

The initialization runs `sgnl-inspiral-bank-splitter` which divides the complete template bank into smaller "SVD bins"
based on a grouping parameter specified in the config.

The SVD splitting can be configured in the `SVD` section of the config, and consists of the following parameters:

- `num_banks`: Number of sub-banks to create per SVD bin
- `num_split_templates`: Number of templates per split bank file
- `sort_by`: Sorting method (`mu`, `chi`, or `mchirp`)
- `num_<param>_bins`: Additional binning if using chi or mu sorting

#### SVD Metadata Generation

The initialization also runs `sgnl-inspiral-set-svdbin-option` which creates an SVD options file containing metadata for each bin:

- Mean/min/max chirp mass and total mass
- Expected template durations
- Autocorrelation lengths
- Gate thresholds for transient noise mitigation

Outputs are placed in the `input_data/split_bank/` directory.

#### SVD Bank Generation

After the initialization has completed and the reference PSD has been generated, we can generate the SVD bank
that is used in the filtering workflow.

#### Outputs and Usage

SVD banks are stored per IFO in `input_data/` and used by:

- **Filter workflow**: Uses the orthogonal basis vectors to efficiently filter data,
then reconstructs matches to original templates using the mixing matrix

The SVD representation is mathematically equivalent to filtering with the full template bank (within the
specified tolerance) but requires far fewer operations.

## Filter Workflow

The filter workflow is the core of the CBC search -- it preforms matched filtering on the detector data using
the SVD banks to identify potential gravitational wave signals. This workflow implements the LLOID
(Low-Latency Online Inspiral Detection) algorithm.

### Overview

The filter workflow consists of two main stages:

1. **Matched filtering**: Process interferometer data through the LLOID algorithm to generate triggers from SNR timeseries
2. **Likelihood ratio marginalization**: Combines likelihood ratio statistics across time segments

### Outputs and Usage

Filter outputs are stored in `filter_dir/` and consumed by:

- **Rank workflow**: Uses triggers and likelihood ratios to compute false alarm rate (FAR) and identify significant candidates

## Rank Workflow

The rank workflow assigns statistical significance to all triggers identified by the filter workflow, computing FARs to determine
which candidates are likely astrophysical signals versus noise. This is the final analysis stage and produces the science-ready
results of the analysis.

### Final Outputs

The rank workflow produces:

1. `FAR_TRIGGERS` databases: Science-ready candidate events with statistical significance
2. `SEGMENTS_FAR_TRIGGERS`: Triggers annotated with detector segment info
3. **Summary webpage**: HTML page with visual results

These are the final products of the offline analysis!
