from specification import Specification
from mso_date_convertor import get_date

import requests
import pandas as pd
import numpy as np
import time
import warnings
warnings.filterwarnings("ignore")


class AdformAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_access_token(self):
        response = requests.post('https://id.adform.com/sts/connect/token',
                                 data={"grant_type": "client_credentials",
                                       "client_id": self.client_id,
                                       "client_secret": self.client_secret,
                                       "scope": "https://api.adform.com/scope/buyer.stats"})
        access_token = str(response.json()["access_token"])
        return access_token

    def get_stat_urls(self, start_num, end_num):
        access_token = self._get_access_token()
        start_date = get_date(start_num)
        end_date = get_date(end_num)
        stat_url_list = []

        for i in range(len(Specification.SPECS)):
            body = {
              "dimensions": ["client", "order", "lineItem", "bannerSize", "rtbAudience", "campaign", "date"],
              "metrics":
              Specification.SPECS[i],
              "filter": {
                "date": {"from": start_date, "to": end_date}
              },
              "paging": {
                "limit": 0
              }
            }
            response = requests.post("https://api.adform.com/v1/buyer/stats/data",
                                     headers={"Content-Type": "application/json",
                                              "Authorization": f"Bearer {access_token}",
                                              "Accept": "application/json"},
                                     json=body)
            endpoint = response.headers["Location"]
            stat_url = f"https://api.adform.com{endpoint}"
            stat_url_list.append(stat_url)
            # Limit na počet vytvořených reportů je 10 za minutu. Reportů je 11, takže je potřeba cyklus zpozdit.
            time.sleep(6)
        return stat_url_list

    def get_stats(self, url_list):
        access_token = self._get_access_token()
        df_stat_all = pd.DataFrame()
        conv_name_rank = 0
        message = []
        # mezi jednotlivými staženími reportů je pro jistotu ještě další zpoždění. Každým cyklem se prodlužuje o sekundu.
        sleep_in_sec = 5
        # Zpoždění mezi vytvořením reportů a stažením prního reportu.
        time.sleep(100)
        for i in range(len(url_list)):
            time.sleep(sleep_in_sec)
            sleep_in_sec += 1
            try:
                response = requests.get(url_list[i], headers={"Content-Type": "application/json",
                                                              "Authorization": f"Bearer {access_token}",
                                                              "Accept": "application/json"})
                response.raise_for_status()
            except requests.HTTPError as http_err:
                message.append(f'HTTP error occurred: {http_err}')
            except Exception as err:
                message.append(f'Other error occurred: {err}')
            else:
                message.append('OK')
                data = response.json()
                df_stat_array = pd.DataFrame.from_dict(data)
                df_stat = pd.DataFrame(np.array(df_stat_array["reportData"][2]),
                                columns=df_stat_array["reportData"][0])
                for i in range(len(Specification.SPECS[i])):
                    conversions_column = i+7
                    metric_name = Specification.CONV_NAME[conv_name_rank]
                    df_stage = pd.DataFrame()
                    df_stage = df_stat.iloc[:,  [0, 1, 2, 3, 4, 5, 6, conversions_column]]
                    df_stage["metric_name"] = metric_name
                    df_stat_all = df_stat_all.append(df_stage,ignore_index=True)
                    conv_name_rank += 1
        return df_stat_all, message
