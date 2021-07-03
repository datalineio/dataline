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


import argparse
import json
import os
import pkgutil
import shutil
from enum import Enum
from typing import Any, Dict

import yaml


class DestinationType(Enum):
    bigquery = "bigquery"
    postgres = "postgres"
    redshift = "redshift"
    snowflake = "snowflake"
    oracle = "oracle"


class TransformConfig:
    def run(self, args):
        inputs = self.parse(args)
        original_config = self.read_json_config(inputs["config"])
        integration_type = inputs["integration_type"]
        transformed_config = self.transform(integration_type, original_config)
        self.write_yaml_config(inputs["output_path"], transformed_config)
        if DestinationType.bigquery.value == integration_type.value:
            # for Bigquery, the credentials should be stored in a separate json file to be used by dbt
            # move it right next to the profile.yml file for easier access.
            shutil.copy("/tmp/bq_keyfile.json", os.path.join(inputs["output_path"], "bq_keyfile.json"))

    @staticmethod
    def parse(args):
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--config", type=str, required=True, help="path to original config")
        parser.add_argument(
            "--integration-type", type=DestinationType, choices=list(DestinationType), required=True, help="type of integration"
        )
        parser.add_argument("--out", type=str, required=True, help="path to output transformed config to")

        parsed_args = parser.parse_args(args)
        print(str(parsed_args))

        return {
            "config": parsed_args.config,
            "integration_type": parsed_args.integration_type,
            "output_path": parsed_args.out,
        }

    def transform(self, integration_type: DestinationType, config: Dict[str, Any]):
        data = pkgutil.get_data(self.__class__.__module__.split(".")[0], "transform_config/profile_base.yml")
        if not data:
            raise FileExistsError("Failed to load profile_base.yml")
        base_profile = yaml.load(data, Loader=yaml.FullLoader)

        transformed_integration_config = {
            DestinationType.bigquery.value: self.transform_bigquery,
            DestinationType.postgres.value: self.transform_postgres,
            DestinationType.redshift.value: self.transform_redshift,
            DestinationType.snowflake.value: self.transform_snowflake,
            DestinationType.oracle.value: self.transform_oracle,
        }[integration_type.value](config)

        # merge pre-populated base_profile with destination-specific configuration.
        base_profile["normalize"]["outputs"]["prod"] = transformed_integration_config

        return base_profile

    @staticmethod
    def transform_bigquery(config: Dict[str, Any]):
        print("transform_bigquery")
        credentials_json = config["credentials_json"]
        keyfile_path = "/tmp/bq_keyfile.json"
        with open(keyfile_path, "w") as fh:
            fh.write(credentials_json)

        # https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile
        dbt_config = {
            "type": "bigquery",
            "method": "service-account",
            "project": config["project_id"],
            "dataset": config["dataset_id"],
            "keyfile": keyfile_path,
            "threads": 32,
            "retries": 1,
        }

        return dbt_config

    @staticmethod
    def transform_postgres(config: Dict[str, Any]):
        print("transform_postgres")
        # https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile
        dbt_config = {
            "type": "postgres",
            "host": config["host"],
            "user": config["username"],
            "pass": config.get("password", ""),
            "port": config["port"],
            "dbname": config["database"],
            "schema": config["schema"],
            "threads": 32,
        }

        return dbt_config

    @staticmethod
    def transform_redshift(config: Dict[str, Any]):
        print("transform_redshift")
        # https://docs.getdbt.com/reference/warehouse-profiles/redshift-profile
        dbt_config = {
            "type": "redshift",
            "host": config["host"],
            "user": config["username"],
            "pass": config["password"],
            "port": config["port"],
            "dbname": config["database"],
            "schema": config["schema"],
            "threads": 32,
        }
        return dbt_config

    @staticmethod
    def transform_snowflake(config: Dict[str, Any]):
        print("transform_snowflake")
        # here account is everything before ".snowflakecomputing.com" as it can include account, region & cloud environment information)
        account = config["host"].replace(".snowflakecomputing.com", "").replace("http://", "").replace("https://", "")
        # https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile
        # snowflake coerces most of these values to uppercase, but if dbt has them as a different casing it has trouble finding the resources it needs. thus we coerce them to upper.
        dbt_config = {
            "type": "snowflake",
            "account": account,
            "user": config["username"].upper(),
            "password": config["password"],
            "role": config["role"].upper(),
            "database": config["database"].upper(),
            "warehouse": config["warehouse"].upper(),
            "schema": config["schema"].upper(),
            "threads": 32,
            "client_session_keep_alive": False,
            "query_tag": "normalization",
        }
        return dbt_config

    @staticmethod
    def transform_oracle(config: Dict[str, Any]):
        print("transform_oracle")
        # https://github.com/techindicium/dbt-oracle#configure-your-profile
        dbt_config = {
            "type": "oracle",
            "host": config["host"],
            "user": config["username"],
            "pass": config["password"],
            "port": config["port"],
            "dbname": config["database"],
            "schema": config["schema"],
            "threads": 32,
        }
        return dbt_config

    @staticmethod
    def read_json_config(input_path: str):
        with open(input_path, "r") as file:
            contents = file.read()
        return json.loads(contents)

    @staticmethod
    def write_yaml_config(output_path: str, config: Dict[str, Any]):
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        with open(os.path.join(output_path, "profiles.yml"), "w") as fh:
            fh.write(yaml.dump(config))


def main(args=None):
    TransformConfig().run(args)
