#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import json
import os
import pathlib
import re
import shutil
import tempfile
from typing import Any, Dict

import pytest
from integration_tests.dbt_integration_test import DbtIntegrationTest
from normalization.destination_type import DestinationType
from normalization.transform_catalog.catalog_processor import CatalogProcessor

temporary_folders = set()

# dbt models and final sql outputs from the following git versioned tests will be written in a folder included in
# airbyte git repository.
git_versioned_tests = ["test_primary_key_streams"]

dbt_test_utils = DbtIntegrationTest()


@pytest.fixture(scope="module", autouse=True)
def before_all_tests(request):
    dbt_test_utils.change_current_test_dir(request)
    dbt_test_utils.setup_db()
    os.environ["PATH"] = os.path.abspath("../.venv/bin/") + ":" + os.environ["PATH"]
    yield
    dbt_test_utils.tear_down_db()
    for folder in temporary_folders:
        print(f"Deleting temporary test folder {folder}")
        shutil.rmtree(folder, ignore_errors=True)


@pytest.fixture
def setup_test_path(request):
    dbt_test_utils.change_current_test_dir(request)
    print(f"Running from: {pathlib.Path().absolute()}")
    print(f"Current PATH is: {os.environ['PATH']}")
    yield
    os.chdir(request.config.invocation_dir)


@pytest.mark.parametrize(
    "test_resource_name",
    set(
        git_versioned_tests
        + [
            # Non-versioned tests outputs below will be written to /tmp folders instead
        ]
    ),
)
@pytest.mark.parametrize("destination_type", list(DestinationType))
# @pytest.mark.parametrize("destination_type", [DestinationType.POSTGRES])
def test_normalization(destination_type: DestinationType, test_resource_name: str, setup_test_path):
    print("Testing normalization")
    integration_type = destination_type.value
    # Create the test folder with dbt project and appropriate destination settings to run integration tests from
    test_root_dir = setup_test_dir(integration_type, test_resource_name)
    destination_config = dbt_test_utils.generate_profile_yaml_file(destination_type, test_root_dir)
    # Use destination connector to create _airbyte_raw_* tables to use as input for the test
    assert setup_input_raw_data(integration_type, test_resource_name, test_root_dir, destination_config)
    # Normalization step
    generate_dbt_models(destination_type, test_resource_name, test_root_dir)
    dbt_test_utils.dbt_run(test_root_dir)
    # Run checks on Tests results
    dbt_test(destination_type, test_resource_name, test_root_dir)
    check_outputs(destination_type, test_resource_name, test_root_dir)


def setup_test_dir(integration_type: str, test_resource_name: str) -> str:
    """
    We prepare a clean folder to run the tests from.

    if the test_resource_name is part of git_versioned_tests, then dbt models and final sql outputs
    will be written to a folder included in airbyte git repository.

    Non-versioned tests will be written in /tmp folders instead.

    The purpose is to keep track of a small set of downstream changes on selected integration tests cases.
     - generated dbt models created by normalization script from an input destination_catalog.json
     - final output sql files created by dbt CLI from the generated dbt models (dbt models are sql files with jinja templating,
     these are interpreted and compiled into the native SQL dialect of the final destination engine)
    """
    if test_resource_name in git_versioned_tests:
        test_root_dir = f"{pathlib.Path().absolute()}/normalization_test_output/{integration_type.lower()}"
    else:
        test_root_dir = f"{pathlib.Path().joinpath('..', 'build', 'normalization_test_output', integration_type.lower()).resolve()}"
    shutil.rmtree(test_root_dir, ignore_errors=True)
    os.makedirs(test_root_dir)
    test_root_dir = f"{test_root_dir}/{test_resource_name}"
    print(f"Setting up test folder {test_root_dir}")
    shutil.copytree("../dbt-project-template", test_root_dir)
    if integration_type.lower() != "redshift":
        # Prefer 'view' to 'ephemeral' for tests so it's easier to debug with dbt
        dbt_test_utils.copy_replace(
            "../dbt-project-template/dbt_project.yml",
            os.path.join(test_root_dir, "dbt_project.yml"),
            pattern="ephemeral",
            replace_value="view",
        )
    else:
        # 'view' materializations on redshift are too slow, so keep it ephemeral there...
        dbt_test_utils.copy_replace("../dbt-project-template/dbt_project.yml", os.path.join(test_root_dir, "dbt_project.yml"))
    return test_root_dir


def setup_input_raw_data(integration_type: str, test_resource_name: str, test_root_dir: str, destination_config: Dict[str, Any]) -> bool:
    """
    We run docker images of destinations to upload test data stored in the messages.txt file for each test case.
    This should populate the associated "raw" tables from which normalization is reading from when running dbt CLI.
    """
    catalog_file = os.path.join("resources", test_resource_name, "data_input", "catalog.json")
    message_file = os.path.join("resources", test_resource_name, "data_input", "messages.txt")
    dbt_test_utils.copy_replace(
        catalog_file,
        os.path.join(test_root_dir, "reset_catalog.json"),
        pattern='"destination_sync_mode": ".*"',
        replace_value='"destination_sync_mode": "overwrite"',
    )
    dbt_test_utils.copy_replace(catalog_file, os.path.join(test_root_dir, "destination_catalog.json"))
    config_file = os.path.join(test_root_dir, "destination_config.json")
    with open(config_file, "w") as f:
        f.write(json.dumps(destination_config))
    commands = [
        "docker",
        "run",
        "--rm",
        "--init",
        "-v",
        f"{test_root_dir}:/data",
        "--network",
        "host",
        "-i",
        f"airbyte/destination-{integration_type.lower()}:dev",
        "write",
        "--config",
        "/data/destination_config.json",
        "--catalog",
    ]
    # Force a reset in destination raw tables
    assert dbt_test_utils.run_destination_process("", test_root_dir, commands + ["/data/reset_catalog.json"])
    # Run a sync to create raw tables in destinations
    return dbt_test_utils.run_destination_process(message_file, test_root_dir, commands + ["/data/destination_catalog.json"])


def generate_dbt_models(destination_type: DestinationType, test_resource_name: str, test_root_dir: str):
    """
    This is the normalization step generating dbt models files from the destination_catalog.json taken as input.
    """
    catalog_processor = CatalogProcessor(os.path.join(test_root_dir, "models", "generated"), destination_type)
    catalog_processor.process(
        os.path.join("resources", test_resource_name, "data_input", "catalog.json"), "_airbyte_data", dbt_test_utils.target_schema
    )


def dbt_test(destination_type: DestinationType, test_resource_name: str, test_root_dir: str):
    """
    dbt provides a way to run dbt tests as described here: https://docs.getdbt.com/docs/building-a-dbt-project/tests
    - Schema tests are added in .yml files from the schema_tests directory
        - see additional macros for testing here: https://github.com/fishtown-analytics/dbt-utils#schema-tests
    - Data tests are added in .sql files from the data_tests directory and should return 0 records to be successful

    We use this mechanism to verify the output of our integration tests.
    """
    replace_identifiers = os.path.join("resources", test_resource_name, "data_input", "replace_identifiers.json")
    copy_test_files(
        os.path.join("resources", test_resource_name, "dbt_schema_tests"),
        os.path.join(test_root_dir, "models/dbt_schema_tests"),
        destination_type,
        replace_identifiers,
    )
    copy_test_files(
        os.path.join("resources", test_resource_name, "dbt_data_tests"),
        os.path.join(test_root_dir, "tests"),
        destination_type,
        replace_identifiers,
    )
    assert dbt_test_utils.run_check_dbt_command("test", test_root_dir)


def check_outputs(destination_type: DestinationType, test_resource_name: str, test_root_dir: str):
    """
    Implement other types of checks on the output directory (grepping, diffing files etc?)
    """
    print("Checking test outputs")


def copy_test_files(src: str, dst: str, destination_type: DestinationType, replace_identifiers: str):
    """
    Copy file while hacking snowflake identifiers that needs to be uppercased...
    (so we can share these dbt tests files accross destinations)
    """
    if os.path.exists(src):
        temp_dir = tempfile.mkdtemp(dir="/tmp/", prefix="normalization_test_")
        temporary_folders.add(temp_dir)
        # Copy and adapt capitalization
        if destination_type.value == DestinationType.SNOWFLAKE.value:
            shutil.copytree(src, temp_dir + "/upper", copy_function=copy_upper)
            src = temp_dir + "/upper"
        elif destination_type.value == DestinationType.REDSHIFT.value:
            shutil.copytree(src, temp_dir + "/lower", copy_function=copy_lower)
            src = temp_dir + "/lower"
        if os.path.exists(replace_identifiers):
            with open(replace_identifiers, "r") as file:
                contents = file.read()
            identifiers_map = json.loads(contents)
            pattern = []
            replace_value = []
            if destination_type.value in identifiers_map:
                for entry in identifiers_map[destination_type.value]:
                    for k in entry:
                        pattern.append(k)
                        replace_value.append(entry[k])
            if pattern and replace_value:

                def copy_replace_identifiers(src, dst):
                    dbt_test_utils.copy_replace(src, dst, pattern, replace_value)

                shutil.copytree(src, temp_dir + "/replace", copy_function=copy_replace_identifiers)
                src = temp_dir + "/replace"
        # final copy
        shutil.copytree(src, dst)


def copy_upper(src, dst):
    print(src, "->", dst)
    dbt_test_utils.copy_replace(
        src,
        dst,
        pattern=[
            r"(- name:) *(.*)",
            r"(ref\(')(.*)('\))",
            r"(source\(')(.*)('\))",
        ],
        replace_value=[
            to_upper_identifier,
            to_upper_identifier,
            to_upper_identifier,
        ],
    )


def copy_lower(src, dst):
    print(src, "->", dst)
    dbt_test_utils.copy_replace(
        src,
        dst,
        pattern=[
            r"(- name:) *(.*)",
            r"(ref\(')(.*)('\))",
            r"(source\(')(.*)('\))",
        ],
        replace_value=[
            to_lower_identifier,
            to_lower_identifier,
            to_lower_identifier,
        ],
    )


def to_upper_identifier(input: re.Match) -> str:
    if len(input.groups()) == 2:
        return f"{input.group(1)} {input.group(2).upper()}"
    elif len(input.groups()) == 3:
        return f"{input.group(1)}{input.group(2).upper()}{input.group(3)}"
    else:
        raise Exception(f"Unexpected number of groups in {input}")


def to_lower_identifier(input: re.Match) -> str:
    if len(input.groups()) == 2:
        return f"{input.group(1)} {input.group(2).lower()}"
    elif len(input.groups()) == 3:
        return f"{input.group(1)}{input.group(2).lower()}{input.group(3)}"
    else:
        raise Exception(f"Unexpected number of groups in {input}")
