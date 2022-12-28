import mso_adform_api as adfapi
import csv
import logging
from datetime import datetime, timedelta

import requests
import json
import re
import pandas as pd
import numpy as np
import time

from pytz import timezone
import warnings
warnings.filterwarnings("ignore")
from retrying import retry


from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_CLIENT_SECRET = '#client_secret'
KEY_CLIENT_ID = 'client_id'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_CLIENT_SECRET, KEY_CLIENT_ID]


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """

        # ####### EXAMPLE TO REMOVE
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters
        # Access parameters in data/config.json
        if params.get(KEY_PRINT_HELLO):
            logging.info("Hello World")

        # get last state data/in/state.json from previous run
        previous_state = self.get_state_file()
        logging.info(previous_state.get('some_state_parameter'))

        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition('conversions.csv', incremental=True, primary_key=['client', 'order', 'lineItem', 'bannerSize', 'rtbAudience', 'campaign', 'date', 'metric_name'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)


        adf = adfapi.AdformAPI("reporting.seznam.cz@clients.adform.com", "0mY2KIYT8OIxi46qqQV_2S5lBR1y6Gl9VjrgFSuD")

        urls = adf.get_stat_urls()
        print(urls)

        @retry(stop_max_attempt_number=2, wait_exponential_multiplier=2000)
        def stats_stop_after_attempts():
            stats_data = adf.get_stats(urls)
            if stats_data[1].count('OK') != 11:
                raise IOError("Stopping after some attempts...")
            else:
                return stats_data

        stats_data_ok = stats_stop_after_attempts()

        print(stats_data_ok[1].count('OK'))
        print(stats_data_ok[1])

        df_conversions = stats_data_ok[0].rename(columns={"conversions": "metric_value"})
            
        df_conversions.to_csv(table.full_path, index=False, encoding = 'utf-8')

        # DO whatever and save into out_table_path
        # with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
        #     writer = csv.DictWriter(out_file, fieldnames=['timestamp'])
        #     writer.writeheader()
        #     writer.writerow({"timestamp": datetime.now().isoformat()})

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        self.write_state_file({"some_state_parameter": "value"})

        # ####### EXAMPLE TO REMOVE END


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
