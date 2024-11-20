import argparse
import collections.abc
import getpass
import pathlib
import sys

import yaml
from ezdag import DAG

from sgnl.dags import layers


def parse_command_line(args: list[str] = None) -> argparse.Namespace:
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Generate a sgnl dag from a config file"
    )
    parser.add_argument(
        "-c", "--config", type=str, required=True, help="The config file to load"
    )
    parser.add_argument(
        "-w", "--workflow", type=str, required=True, help="The type of dag to generate"
    )
    parser.add_argument(
        "--dag-dir",
        type=str,
        default=".",
        help="The directory in which to write the dag",
    )
    parser.add_argument("--dag-name", type=str, default=None, help="A name for the dag")
    args = parser.parse_args(args)

    if args.dag_name:
        if "/" in args.dag_name:
            raise ValueError(
                "The dag name must not be a path. Use --dag-dir to put the dag in a different directory."
            )
        if args.dag_name.endswith(".dag"):
            raise ValueError(
                'The given dag name ends with ".dag". This is unnecessary because it will be appended to the file name automatically.'
            )

    return args


class DotDict(dict):
    """
    A dictionary supporting dot notation.
    """

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = DotDict(v)


def replace_hyphens(dict_, reverse=False):
    """
    Replace hyphens in key names with underscores
    """
    out = dict(dict_)
    for k, v in out.items():
        if isinstance(v, dict):
            out[k] = replace_hyphens(v)
    return {
        k.replace("_", "-") if reverse else k.replace("-", "_"): v
        for k, v in out.items()
    }


def recursive_update(d, u):
    """
    Recursively update a dictionary d from a dictionary u
    """
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = recursive_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def build_config(config_path, dag_dir):
    # Load default config
    path_to_dags = pathlib.Path(layers.__file__).parent
    default_config_file = path_to_dags / "default_config.yml"
    with open(default_config_file.as_posix(), "r") as file:
        default_config_yaml = yaml.safe_load(file)

    # Handle empty default config
    if default_config_yaml is None:
        default_config_yaml = {}

    default_config = replace_hyphens(default_config_yaml)

    # Load input config
    with open(config_path, "r") as file:
        config_in = replace_hyphens(yaml.safe_load(file))

    # Ensure presence of options required by all dags
    config_in = DotDict(config_in)
    assert config_in.condor, "The config is missing the condor section"
    assert (
        config_in.condor.accounting_group
    ), "The condor section of the config must specify an accounting-group"
    assert (
        config_in.condor.container
    ), "The condor section of the config must specify a container"

    # Overwrite default config values with those from the input config
    config = DotDict(recursive_update(default_config, config_in))

    # Set a few more config options derived from inputs
    if not config.paths:
        config.paths = DotDict({})
    if not config.paths.storage:
        config.paths.storage = dag_dir
    if not config.condor.accounting_group_user:
        config.condor.accounting_group_user = getpass.getuser()

    return config


def main():
    args = parse_command_line()
    config = build_config(args.config, args.dag_dir)

    if args.dag_name:
        dag_name = args.dag_name
    else:
        dag_name = f"sgnl_{args.workflow}"

    # Start building the dag
    dag = DAG(dag_name)
    dag.create_log_dir()
    if args.workflow == "test":
        # FIXME Delete this workflow
        dag.attach(layers.test(config.echo, config.condor))

    if args.workflow == "psd":
        # Reference PSD layer
        dag.attach(layers.reference_psd(config.psd, config.condor))

        # Median PSD layer
        dag.attach(layers.median_psd(config.psd, config.condor))

    if args.workflow == "svd":
        pass

    # Write dag and script to disk
    dag.write(pathlib.Path(args.dag_dir), write_script=True)


if __name__ == "__main__":
    main()
