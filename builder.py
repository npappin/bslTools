#!/usr/bin/env python3

import requests, json, os
from tqdm import tqdm

def download():
    s = requests.Session()
    s.headers.update({'User-Agent': 'bslTools'})
    response = s.get("https://broadbandmap.fcc.gov/nbm/map/api/published/filing")
    parsed = json.loads(response.text)
    uuid = parsed['data'][0]['process_uuid']
    url = f'https://broadbandmap.fcc.gov/nbm/map/api/national_map_process/nbm_get_data_download/{uuid}'
    response = s.get(url)
    parsed = json.loads(response.text)
    dataToProcess = parsed['data']
    dataToProcess = [item for item in dataToProcess if item['state_name'] != None]
    dataToProcess = [item for item in dataToProcess if item['file_type'] == 'csv']
    if not os.path.isdir('data'):
        os.makedirs('data')
        if not os.path.isdir(os.path.join('data', 'zips')):
            os.makedirs(os.path.join('data', 'zips'))

    cachedFiles = os.listdir(os.path.join('data', 'zips'))
    cachedFiles = [entry for entry in cachedFiles if entry.endswith(".zip")]
    cachedFileNames = [x.split('.')[0] for x in cachedFiles]

    for item in tqdm(dataToProcess):
        # print(item)
        if item['file_name'] not in cachedFileNames:
            url = f"https://broadbandmap.fcc.gov/nbm/map/api/getNBMDataDownloadFile/{item['id']}/1"
            r = s.get(url)
            filename = f'{os.path.join("data", "zips", item["file_name"])}.zip'
            open(filename, 'wb').write(r.content)
    return True

def main():
    download()
    return True

if __name__ == "__main__":
    main()
