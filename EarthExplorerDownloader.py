"""
1. 讀取帳密
2. 讀取query.csv
3. import landsat api and search image name
4. save image name list to download list
5. start download images
"""

import json
import csv
import landsatxplore.api as lse
import asyncio

class EarthExplorerDownloader(object):
    SETTINGS_PATH = "./settings.json"
    
    def __init__(self):
        with open(self.SETTINGS_PATH) as f:
            settings = json.load(f)

        self.user_account = settings['user']['account']
        self.user_password = settings['user']['password']
        self.query_csv_path = settings['path']['query_csv']
        self.save_searched_info_path = settings['path']['save_searched_info_csv']

    def __read_query_csv(self):
        queryDicts = []
        with open(self.query_csv_path) as f:
            rows = csv.DictReader(f)
            queryDicts = [ d for d in rows if d["dataset"][0] != '#' ]
        return queryDicts

    def __search_scenes(self, api, qd):
        scenes = api.search(
            dataset = str.strip(qd['dataset']),
            latitude = float(str.strip(qd['latitude'])),
            longitude = float(str.strip(qd['longitude'])),
            start_date = str.strip(qd['start_date']),
            end_date = str.strip(qd['end_date']),
            max_cloud_cover = int(str.strip(qd['max_cloud_cover'])))
        matchedScenes = []
        for s in scenes:
            id = s['displayId'].split('_')[2]
            if id == qd['field']:
                matchedScenes.append(s)
        return matchedScenes

    def __search_images(self, queryDicts):
        api = lse.API(self.user_account, self.user_password)
        imgInfos = []
        for qd in queryDicts:
            imgInfos += self.__search_scenes(api, qd)
        api.logout()
        return imgInfos

    def __save2csv(self, imagesInfo):
        fieldnames=["displayId", "acquisitionDate", "browseUrl", "cloudCover", "dataAccessUrl", 
                    "downloadUrl", "endTime", "entityId", "fgdcMetadataUrl", "metadataUrl", "modifiedDate",
                    "orderUrl", "sceneBounds", "sceneBounds", "startTime"]
        with open(self.save_searched_info_path, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()
            for imgInfo in imagesInfo:
                writer.writerow({ field: imgInfo[field] for field in fieldnames })

    def go(self):
        queryDicts = self.__read_query_csv()
        imgInfos = self.__search_images(queryDicts)
        self.__save2csv(imgInfos)
        print('Finish!! Total images:', len(imgInfos))

downloader = EarthExplorerDownloader()
downloader.go()
