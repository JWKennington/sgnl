from dataclasses import dataclass

from ezdag import DAG, Argument, Layer, Node

# from gstlal import plugins
# from gstlal.datafind import DataType, DataCache
# from gstlal.dags import Argument, Option
# from gstlal.dags import util as dagutil
# from gstlal.dags.layers import Layer, Node


def create_layer(executable, condor_config, resource_requests):
    # Default submit file options
    submit_description = create_submit_description(condor_config)

    # Add resource requests
    submit_description.update(resource_requests)

    # Allow arbitrary submit file options from the config
    if executable in condor_config:
        submit_description.update(condor_config[executable])

    # Set file transfer
    if condor_config.transfer_files:
        transfer_files = condor_config.transfer_files
    else:
        transfer_files = True

    return Layer(
        executable,
        transfer_files=transfer_files,
        submit_description=submit_description,
    )


def create_submit_description(condor_config):
    submit_description = {
        "want_graceful_removal": "True",
        "kill_sig": "15",
        "accounting_group": condor_config.accounting_group,
        "accounting_group_user": condor_config.accounting_group_user,
    }
    requirements = []
    environment = {}

    # Container options
    if "container" in condor_config:
        singularity_image = condor_config["container"]
        submit_description["MY.SingularityImage"] = f'"{condor_config.container}"'
        submit_description["transfer_executable"] = False

    # Scitoken options
    if condor_config.use_scitokens == True:
        submit_description["use_oauth_services"] = "scitokens"
        environment["BEARER_TOKEN_FILE"] = (
            f"$$(CondorScratchDir)/.condor_creds/{submit_description['use_oauth_services']}.use"
        )

    # Config options
    if "directives" in condor_config:
        submit_description.update(condor_config["directives"])
    if "requirements" in condor_config:
        requirements.extend(condor_config["requirements"])
    if "environment" in condor_config:
        environment.update(condor_config["environment"])

    # Condor requirements
    submit_description["requirements"] = " && ".join(requirements)

    # Condor environment
    env_opts = [f"{key}={val}" for (key, val) in environment.items()]
    if "environment" in submit_description:
        env_opts.append(submit_description["environment"].strip('"'))
    submit_description["environment"] = f'"{" ".join(env_opts)}"'

    return submit_description


def test(echo_config, condor_config):
    # FIXME Delete this layer
    executable = "echo"
    resource_requests = {
        "request_cpus": 1,
        "request_memory": "10MB",
        "request_disk": "10MB",
    }
    layer = create_layer(executable, condor_config, resource_requests)

    for i in range(echo_config.jobs):
        arguments = [Argument("words", f"This is job {i}")]
        inputs = [Argument("file_in", "inputs/file.txt")]
        outputs = [Argument("file_out", "outputs/output.txt")]
        layer += Node(arguments=arguments, inputs=inputs, outputs=outputs)

    return layer


def reference_psd(psd_config, condor_config, psd_cache):
    pass


def median_psd(psd_config, condor_config, psd_cache):
    pass
