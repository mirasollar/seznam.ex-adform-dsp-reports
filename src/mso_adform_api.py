from specification import Specification

import requests
import json
import re
import pandas as pd
import numpy as np
import csv
import time
from datetime import datetime, timedelta
from pytz import timezone
import warnings
warnings.filterwarnings("ignore")
from retrying import retry

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

    def get_stat_urls(self):
      access_token = self._get_access_token()
      stat_url_list = []

      for i in range(len(Specification.SPECS)):
        body = {
        "dimensions": [
          "client",
          "order",
          "lineItem",
          "bannerSize",
          "rtbAudience",
          "campaign",
          "date"
        ],
        "metrics": 
          Specification.SPECS[i]
        ,
          "filter": {
            "date": {"from": '2022-12-25', "to": '2022-12-25'}
          },
          "paging": {
              "limit": 0
          }
        }
        response = requests.post("https://api.adform.com/v1/buyer/stats/data",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {access_token}",  "Accept": "application/json"},
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
          response = requests.get(url_list[i],
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {access_token}", "Accept": "application/json"})
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

if __name__ == "__main__":
  adf = AdformAPI("reporting.seznam.cz@clients.adform.com", "0mY2KIYT8OIxi46qqQV_2S5lBR1y6Gl9VjrgFSuD")

  urls = adf.get_stat_urls()
  print(urls)

  # stats_data_ok = adf.get_stats(urls)

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
    
  df_conversions.to_csv('conversions.csv', index=False, encoding = 'utf-8')

  # https://stackoverflow.com/questions/58140435/how-to-organize-python-api-module-to-make-it-neat