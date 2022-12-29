import mso_adform_api as adfapi
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
import logging
from retrying import retry
import warnings
warnings.filterwarnings("ignore")


# configuration variables
KEY_CLIENT_ID = 'client_id'
KEY_CLIENT_SECRET = '#client_secret'
KEY_DATE_RANGE = 'date_range'
KEY_START_NUM = 'start_num'
KEY_END_NUM = 'end_num'
KEY_INCREMENTAL = 'incremental_output'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_CLIENT_SECRET, KEY_DATE_RANGE, KEY_INCREMENTAL]


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
        if params.get(KEY_CLIENT_SECRET):
            logging.info('Loading configuration...')

        # get last state data/in/state.json from previous run
        # previous_state = self.get_state_file()
        # logging.info(previous_state.get('some_state_parameter'))

        # Create output table (Tabledefinition - just metadata)
        incremental = params.get(KEY_INCREMENTAL)
        table = self.create_out_table_definition('conversions.csv',
                                                 incremental=incremental,
                                                 primary_key=['client',
                                                              'order',
                                                              'lineItem',
                                                              'bannerSize',
                                                              'rtbAudience',
                                                              'campaign',
                                                              'date',
                                                              'metric_name'])

        logging.info('Extracting reports from Adform...')
        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)
        adf = adfapi.AdformAPI(client_id, client_secret)

        date_range = params[KEY_DATE_RANGE]
        start_num = date_range[KEY_START_NUM]
        end_num = date_range[KEY_END_NUM]
        urls = adf.get_stat_urls(start_num, end_num)

        @retry(stop_max_attempt_number=5, wait_exponential_multiplier=2000)
        def stats_stop_after_attempts():
            stats_data = adf.get_stats(urls)
            if stats_data[1].count('OK') != 11:
                raise IOError("Stopping after some attempts...")
            else:
                return stats_data

        stats_data_ok = stats_stop_after_attempts()

        df_conversions = stats_data_ok[0].rename(columns={"conversions": "metric_value"})
        df_conversions.to_csv(table.full_path, index=False, encoding='utf-8')

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        self.write_state_file({"some_state_parameter": "value"})


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
