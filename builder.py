#!/usr/bin/env python3

import requests, json, os, shutil, sys
from tqdm import tqdm
import pandas as pd


def download():
    print("Downloading files")
    s = requests.Session()
    s.headers.update({"User-Agent": "bslTools"})
    response = s.get("https://broadbandmap.fcc.gov/nbm/map/api/published/filing")
    parsed = json.loads(response.text)
    uuid = parsed["data"][0]["process_uuid"]
    url = f"https://broadbandmap.fcc.gov/nbm/map/api/national_map_process/nbm_get_data_download/{uuid}"
    response = s.get(url)
    parsed = json.loads(response.text)
    dataToProcess = parsed["data"]
    dataToProcess = [item for item in dataToProcess if item["state_name"] != None]
    dataToProcess = [item for item in dataToProcess if item["file_type"] == "csv"]
    if not os.path.isdir("data"):
        os.makedirs("data")
        if not os.path.isdir(os.path.join("data", "zips")):
            os.makedirs(os.path.join("data", "zips"))

    cachedFiles = os.listdir(os.path.join("data", "zips"))
    cachedFiles = [entry for entry in cachedFiles if entry.endswith(".zip")]
    cachedFileNames = [x.split(".")[0] for x in cachedFiles]

    for item in tqdm(dataToProcess):
        # print(item)
        if item["file_name"] not in cachedFileNames:
            url = f"https://broadbandmap.fcc.gov/nbm/map/api/getNBMDataDownloadFile/{item['id']}/1"
            r = s.get(url)
            filename = f'{os.path.join("data", "zips", item["file_name"])}.zip'
            open(filename, "wb").write(r.content)
    return True


def prep():
    print("Preparing Files")
    fileList = os.listdir(os.path.join("data", "zips"))
    fileList = [f for f in fileList if f.endswith(".zip")]
    for file in tqdm(fileList):
        # fileName = f'{file}'
        state = file.split("_")[1]
        if not os.path.isdir(os.path.join("data", state)):
            os.makedirs(os.path.join("data", state))
        shutil.unpack_archive(
            os.path.join("data", "zips", file), os.path.join("data", state)
        )
        # os.remove(file)

    # Fix Ohio
    here = os.path.join(
        "data",
        "39",
        "bdc_39_Licensed-Fixed-Wireless_fixed_broadband_063022",
        "bdc_39_Licensed-Fixed-Wireless_fixed_broadband_063022.csv",
    )
    there = os.path.join("data", "39")
    if os.path.isdir(
        os.path.join(
            "data", "39", "bdc_39_Licensed-Fixed-Wireless_fixed_broadband_063022"
        )
    ):
        shutil.move(here, there)
        os.rmdir(
            os.path.join(
                "data", "39", "bdc_39_Licensed-Fixed-Wireless_fixed_broadband_063022"
            )
        )
    return True


def buildStates():
    print("Building state level data")
    folderList = os.listdir("data")
    dropItems = ["zips", ".DS_Store"]
    folderList = [f for f in folderList if f not in dropItems]
    # print(folderList)
    for folder in tqdm(folderList):
        # Do state stuff
        fileList = os.listdir(os.path.join("data", folder))
        fileList = [f for f in fileList if f.endswith(".csv")]
        # print(fileList)
        stateData = pd.DataFrame()
        for file in tqdm(fileList):
            df = pd.DataFrame()
            df = pd.read_csv(os.path.join("data", folder, file))
            # os.remove(file)
            stateName = df.state_usps.unique()[0].lower()
            columnsToDrop = [
                "frn",
                "provider_id",
                "brand_name",
                "technology",
                "max_advertised_download_speed",
                "max_advertised_upload_speed",
                "low_latency",
                "business_residential_code",
                "state_usps",
            ]
            df = df.drop(columns=columnsToDrop)
            df.drop_duplicates(inplace=True)
            stateData = pd.concat([stateData, df], ignore_index=True)
            # print(df)
        print(f"\n{stateName}\n")
        stateData.drop_duplicates(inplace=True)
        stateData.reset_index(drop=True, inplace=True)
        dfH3 = pd.DataFrame(stateData)
        dfBlock = pd.DataFrame(stateData)
        bslLookup = pd.DataFrame(stateData)
        dfH3.drop(columns=["block_geoid"], inplace=True)
        dfH3.drop_duplicates(inplace=True)
        dfBlock.drop(columns=["h3_res8_id"], inplace=True)
        dfBlock.drop_duplicates(inplace=True)
        dfH3.drop(columns=["location_id"], inplace=True)
        dfBlock.drop(columns=["location_id"], inplace=True)
        dfH3 = (
            dfH3.groupby("h3_res8_id").size().reset_index().rename(columns={0: "bsls"})
        )
        dfBlock = (
            dfBlock.groupby("block_geoid")
            .size()
            .reset_index()
            .rename(columns={0: "bsls"})
        )
        if not os.path.isdir(os.path.join("states")):
            os.mkdir(os.path.join("states"))
        dfH3.to_csv(os.path.join("states", f"{stateName}H3Bsls.csv"), index=False)
        dfH3.to_parquet(
            os.path.join("states", f"{stateName}H3Bsls.parquet"), index=False
        )
        dfBlock.to_csv(os.path.join("states", f"{stateName}BlockBsls.csv"), index=False)
        dfBlock.to_parquet(
            os.path.join("states", f"{stateName}BlockBsls.parquet"), index=False
        )
        bslLookup.to_csv(
            os.path.join("states", f"{stateName}BslLookup.csv"), index=False
        )
        bslLookup.to_parquet(
            os.path.join("states", f"{stateName}BslLookup.parquet"), index=False
        )
    pass


def buildNational():
    print("Building national level data")
    fileList = os.listdir("states")
    fileList = [f for f in fileList if f.endswith(".parquet")]
    lookupList = [f for f in fileList if f.endswith("Lookup.parquet")]
    blockList = [f for f in fileList if f.endswith("BlockBsls.parquet")]
    h3List = [f for f in fileList if f.endswith("H3Bsls.parquet")]
    lookup = pd.DataFrame()
    print("lookup")
    for f in tqdm(lookupList):
        df = pd.DataFrame()
        df = pd.read_parquet(os.path.join("states", f))
        lookup = pd.concat([lookup, df], ignore_index=True)
    if not os.path.isdir("national"):
        os.makedirs("national")
    lookup.to_csv(os.path.join("national", "bslsLookup.csv"), index=False)
    lookup.to_parquet(os.path.join("national", "bslsLookup.parquet"), index=False)
    del lookup
    blockBsls = pd.DataFrame()
    print("block")
    for f in tqdm(blockList):
        df = pd.DataFrame()
        df = pd.read_parquet(os.path.join("states", f))
        blockBsls = pd.concat([blockBsls, df], ignore_index=True)
    blockBsls = blockBsls.groupby("block_geoid").sum().reset_index()
    blockBsls.to_csv(os.path.join("national", "blockBsls.csv"), index=False)
    blockBsls.to_parquet(os.path.join("national", "blockBsls.parquet"), index=False)
    del blockBsls
    h3Bsls = pd.DataFrame()
    print("h3")
    for f in tqdm(h3List):
        df = pd.DataFrame()
        df = pd.read_parquet(os.path.join("states", f))
        h3Bsls = pd.concat([h3Bsls, df], ignore_index=True)
    h3Bsls = h3Bsls.groupby("h3_res8_id").sum().reset_index()
    h3Bsls.to_csv(os.path.join("national", "h3Bsls.csv"), index=False)
    h3Bsls.to_parquet(os.path.join("national", "h3Bsls.parquet"), index=False)
    pass


def main():
    download()
    prep()
    buildStates()
    buildNational()
    return True


if __name__ == "__main__":
    main()
