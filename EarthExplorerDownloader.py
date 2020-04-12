"""
1. 讀取帳密
2. 讀取query.csv
3. import landsat api and search image name
4. save image name list to download list
5. start download images
"""

import json
import csv

class EarthExplorerDownloader(object):
    SETTINGS_PATH = "./settings.json"
    
    def __init__(self):
        with open(self.SETTINGS_PATH) as f:
            settings = json.load(f)

        self.user_account = settings['user']['account']
        self.user_password = settings['user']['password']

    def __read_query_csv(self):
        queryDicts = []
        with open("./query.csv") as f:
            rows = csv.DictReader(f)
            queryDicts = [ d for d in rows ]
        return queryDicts

    def go(self):
        queryDicts = self.__read_query_csv()
        print(queryDicts[0]['dataset'])


downloader = EarthExplorerDownloader()
downloader.go()
