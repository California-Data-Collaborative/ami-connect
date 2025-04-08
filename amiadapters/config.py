import os

from dotenv import load_dotenv


class AMIAdapterConfiguration:

    def __init__(self, **kwargs):
        self.utility_name = kwargs.get("utility_name")
        self.output_folder = kwargs.get("output_folder")
        self.sentryx_api_key = kwargs.get("sentryx_api_key")
        self.beacon_360_user = kwargs.get("beacon_360_user")
        self.beacon_360_password = kwargs.get("beacon_360_password")
        self.snowflake_user = kwargs.get("snowflake_user")
        self.snowflake_password = kwargs.get("snowflake_password")
        self.snowflake_account = kwargs.get("snowflake_account")
        self.snowflake_warehouse = kwargs.get("snowflake_warehouse")
        self.snowflake_database = kwargs.get("snowflake_database")
        self.snowflake_schema = kwargs.get("snowflake_schema")
        self.snowflake_role = kwargs.get("snowflake_role")

    @classmethod
    def from_env(cls):
        # Assumes .env file in working directory
        load_dotenv()
        return AMIAdapterConfiguration(
            utility_name=os.environ.get("UTILITY_NAME"),
            output_folder=os.environ.get("AMI_DATA_OUTPUT_FOLDER"),
            sentryx_api_key=os.environ.get("SENTRYX_API_KEY"),
            beacon_360_user=os.environ.get("BEACON_AUTH_USER"),
            beacon_360_password=os.environ.get("BEACON_AUTH_PASSWORD"),
            snowflake_user=os.environ.get("SNOWFLAKE_USER"),
            snowflake_password=os.environ.get("SNOWFLAKE_PASSWORD"),
            snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            snowflake_database=os.environ.get("SNOWFLAKE_DATABASE"),
            snowflake_schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            snowflake_role=os.environ.get("SNOWFLAKE_ROLE"),
        )
