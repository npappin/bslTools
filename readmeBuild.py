#!/usr/bin/env python3

import os

stateList = [f[0:2] for f in os.listdir('states')]
stateList = list(set(stateList))
stateList.sort()

url = 'https://pub-96372591292d4fdca85ff0f6db6c67c2.r2.dev/bslTools/states'

for state in stateList:
    lookupStr = f'[CSV]({url}/{state}BslLookup.csv) / [Parquet]({url}/{state}BslLookup.parquet)'
    blockStr = f'[CSV]({url}/{state}BlockBsls.csv) / [Parquet]({url}/{state}BlockBsls.parquet)'
    h3Str = f'[CSV]({url}/{state}H3Bsls.csv) / [Parquet]({url}/{state}H3Bsls.parquet)'
    line = f'| {state.upper()} | {lookupStr} | {blockStr} | {h3Str} |'
    print(line)