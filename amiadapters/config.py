import os

from dotenv import load_dotenv


class AMIAdapterConfiguration:

    def __init__(self, **kwargs):
        self.output_folder = kwargs.get('output_folder')
        self.sentryx_api_key = kwargs.get('sentryx_api_key')
        self.beacon_360_user = kwargs.get('beacon_360_user')
        self.beacon_360_password = kwargs.get('beacon_360_password')

    @classmethod
    def from_env(cls):
        # Assumes .env file in working directory
        load_dotenv()
        output_folder = os.environ.get('AMI_DATA_OUTPUT_FOLDER')
        sentryx_api_key = os.environ.get('SENTRYX_API_KEY')
        beacon_360_user = os.environ.get('BEACON_AUTH_USER')
        beacon_360_password = os.environ.get('BEACON_AUTH_PASSWORD')
        return AMIAdapterConfiguration(
            output_folder=output_folder,
            sentryx_api_key=sentryx_api_key,
            beacon_360_user=beacon_360_user,
            beacon_360_password=beacon_360_password,
        )
