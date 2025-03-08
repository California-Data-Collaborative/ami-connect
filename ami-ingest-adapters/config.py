import os

from dotenv import load_dotenv


class AMIAdapterConfiguration:

    def __init__(self, **kwargs):
        self.output_folder = kwargs.get('output_folder')
        self.sentryx_api_key = kwargs.get('sentryx_api_key')

    @classmethod
    def from_env(cls):
        # Assumes .env file in working directory
        load_dotenv()
        output_folder = os.environ.get('AMI_DATA_OUTPUT_FOLDER')
        sentryx_api_key = os.environ.get('SENTRYX_API_KEY')
        return AMIAdapterConfiguration(
            output_folder=output_folder,
            sentryx_api_key=sentryx_api_key,
        )
