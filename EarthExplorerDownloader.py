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
from time import sleep

class EarthExplorerDownloader(object):
    SETTINGS_PATH = "./settings.json"
    
    def __init__(self):
        with open(self.SETTINGS_PATH) as f:
            settings = json.load(f)
        self.username = settings['user']['username']
        self.password = settings['user']['password']
        self.query_csv_path = settings['path']['query_csv']
        self.download_dir = settings['path']['download_dir']
        self.fail_txt_path = settings['path']['fail_list_txt']
        self.max_threads = settings['max_threads']

    def __read_query_csv(self, csv_path):
        query_dicts = []
        with open(csv_path) as f:
            rows = csv.DictReader(f)
            query_dicts = [ d for d in rows if d["dataset"][0] != '#' ]
        return query_dicts

    def __search_scenes(self, api, qd):
        scenes = api.search(
            dataset = str.strip(qd['dataset']),
            latitude = float(str.strip(qd['latitude'])),
            longitude = float(str.strip(qd['longitude'])),
            start_date = str.strip(qd['start_date']),
            end_date = str.strip(qd['end_date']),
            max_cloud_cover = int(str.strip(qd['max_cloud_cover'])))
        matched_scenes = []
        for s in scenes:
            id = s['displayId'].split('_')[2]
            if id == qd['field']:
                matched_scenes.append(s)
        return matched_scenes

    def __search(self, query_dicts):
        api = lse.API(self.username, self.password)
        imgInfos = []
        for qd in query_dicts:
            imgInfos += self.__search_scenes(api, qd)
        api.logout()
        return imgInfos

    def __save2csv(self, scene_infos, csv_path):
        if not scene_infos:
            return
        fieldnames = [ s for s in scene_infos[0] ]
        with open(csv_path, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()
            for si in scene_infos:
                writer.writerow({ field: si[field] for field in fieldnames })

    def __download_scene(self, scene_id, output_dir):
        try:
            ee = EarthExplorer(self.username, self.password)
            ee.download(scene_id, output_dir)
            ee.logout()
            return scene_id, True
        except Exception:
            scene_file = f'{output_dir}/{scene_id}.tar.gz'
            if os.path.exists(scene_file):
                os.remove(scene_file)
            return scene_id, False
    
    async def __download_scene_async(self, executor, scene_id, output_dir):
        sleep(1)
        return await asyncio.get_running_loop().run_in_executor(executor, self.__download_scene, scene_id, output_dir)

    def __create_output_folder(self):
        if not os.path.exists(self.download_dir):
            os.mkdir(self.download_dir)

    def __download(self, scene_ids):
        max_threads = self.max_threads if self.max_threads > 0 else None
        executor = ccrtf.ThreadPoolExecutor(max_workers=max_threads)
        loop = asyncio.get_event_loop()
        tasks = []
        for scene_id in scene_ids:
            task = loop.create_task(self.__download_scene_async(executor, scene_id, self.download_dir))
            tasks.append(task)
        loop.run_until_complete(asyncio.wait(tasks))
        return [ { 'scene_id': t.result()[0], 'is_successful': t.result()[1] } for t in tasks ]

    def __save2txt(self, error_list, txt_path):
        error_list = [ f'{t}\n' for t in error_list ]
        with open(txt_path, 'w') as file:
            file.writelines(error_list)

    def start_download_flow(self, scene_ids):
        if len(scene_ids):
            print('Start to download...')
            self.__create_output_folder()
            results = self.__download(scene_ids)
            success_cases = [ r for r in results if r['is_successful'] ]
            fail_cases = [ r for r in results if not r['is_successful'] ]
            print(f'Download {len(results)} files, {len(success_cases)} successful and {len(fail_cases)} failed.')
            print(f'Record the failed cases to {self.fail_txt_path}')
            self.__save2txt([ f['scene_id'] for f in fail_cases ], self.fail_txt_path)
            print('Download finished.')

    def go(self):
        if os.path.exists(self.fail_txt_path):
            with open(self.fail_txt_path) as file:
                failed_scene_ids = file.readlines()
            print(f'偵測到上一次下載失敗的{len(failed_scene_ids)}個Cases')
            redownload_ans = input(f'請問是否需要重新下載(Y/N):').upper()
            if redownload_ans == 'Y' :
                failed_scene_ids = [ s.strip('\n') for s in failed_scene_ids ]
                self.start_download_flow(failed_scene_ids)
                return
            elif redownload_ans != 'N':
                return

        print(f'Read csv: {self.query_csv_path}')
        query_dicts = self.__read_query_csv(self.query_csv_path)
        print('Start to search...')
        scene_infos = self.__search(query_dicts)
        print(f'{len(scene_infos)} scences found !!')
        self.start_download_flow([ sceneInfo['displayId'] for sceneInfo in scene_infos ])


downloader = EarthExplorerDownloader()
downloader.go()
