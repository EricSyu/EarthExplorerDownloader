"""
EarthExplorerDownloader

"""

import json
import csv
import landsatxplore.api as lse
import asyncio
from landsatxplore.earthexplorer import EarthExplorer
import os
import concurrent.futures as ccrtf
from random import randint
from time import sleep

class EarthExplorerDownloader(object):
    SETTINGS_PATH = "./settings.json"
    
    def __init__(self):
        with open(self.SETTINGS_PATH) as f:
            settings = json.load(f)
        self.username = settings['user']['username']
        self.password = settings['user']['password']
        self.query_csv_path = settings['path']['query_csv']
        self.output_dir = settings['path']['output_dir']
        self.max_threads = settings['max_threads']

    def __read_query_csv(self, csvPath):
        queryDicts = []
        with open(csvPath) as f:
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

    def __search(self, queryDicts):
        api = lse.API(self.username, self.password)
        imgInfos = []
        for qd in queryDicts:
            imgInfos += self.__search_scenes(api, qd)
        api.logout()
        return imgInfos

    def __save2csv(self, sceneInfos, csvPath):
        if not sceneInfos:
            return
        fieldnames = [ s for s in sceneInfos[0] ]
        with open(csvPath, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()
            for si in sceneInfos:
                writer.writerow({ field: si[field] for field in fieldnames })

    def __download_scene(self, scene_id, output_dir):
        try:
            ee = EarthExplorer(self.username, self.password)
            ee.download(scene_id, output_dir)
            ee.logout()
            return True
        except Exception:
            scene_file = f'{output_dir}/{scene_id}.tar.gz'
            if os.path.exists(scene_file):
                os.remove(scene_file)
            return False
    
    async def __download_scene_async(self, executor, scene_id, output_dir):
        sleep(1)
        return await asyncio.get_running_loop().run_in_executor(executor, self.__download_scene, scene_id, output_dir)

    def __create_output_folder(self):
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

    def __download(self, scene_ids):
        max_threads = self.max_threads if self.max_threads > 0 else None
        executor = ccrtf.ThreadPoolExecutor(max_workers=max_threads)
        loop = asyncio.get_event_loop()
        tasks = []
        for scene_id in scene_ids:
            task = loop.create_task(self.__download_scene_async(executor, scene_id, self.output_dir))
            tasks.append(task)
        loop.run_until_complete(asyncio.wait(tasks))

    def go(self):
        print(f'Read csv: {self.query_csv_path}')
        queryDicts = self.__read_query_csv(self.query_csv_path)
        print('Start to search...')
        sceneInfos = self.__search(queryDicts)
        print(f'{len(sceneInfos)} scences found !!')
        if sceneInfos:
            print('Start to download...')
            self.__create_output_folder()
            self.__download([ sceneInfo['displayId'] for sceneInfo in sceneInfos ])
        print('Finish.')
        

downloader = EarthExplorerDownloader()
downloader.go()
