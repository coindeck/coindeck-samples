import asyncio
import datetime
import json
import threading

import async_timeout
import dateutil.parser as dp
import dateutil
import dateutil.parser
import calendar
import time

from typing import List

from coinlib import pip_install_or_ignore
from coinlib.feature.CoinlibFeatureFetcher import CoinlibFeatureFetcher
from coinlib.feature.CoinlibFeatureLake import CoinlibFeatureLake
from coinlib.feature.CoinlibFeatureProcessor import CoinlibFeatureProcessor
from coinlib.feature.FeatureDTO import FeatureData, ProcessResponse, FeatureLakeData, FeatureFetcherDTO
from coinlib.feature.Features import FeatureModuleType

import requests
import pandas as pd
import coinlib as c
import websockets
from coinlib.logics.LogicJob import LogicJob
import time

##version: 1.0.0

c.connectAsFeature()

class DataFeatureFetcherSample(CoinlibFeatureFetcher):

    def __init__(self):
        super().__init__()
        pass


    async def get_feature_infos(self) -> List[FeatureData]:
        list: List[FeatureData] = [
            FeatureData.create("example_id", "DataFeauture Fetcher example",  description="Rank inside of Your Fetcher",
                               symbol=True, group="example"),
        ]

        return list

    async def fetch_data(self, data: FeatureFetcherDTO) -> ProcessResponse:
        p = ProcessResponse()

        self.write_data("example_id", str(time.time()), group="example")

        return p


c.features.registerFeature(DataFeatureFetcherSample, "featurefetcher", "Feature Fetcher Example",
                           estimatedDataInterval=24*60*60,
                           type=FeatureModuleType.fetcher,
                           options=[])


c.simulator.simulateFeature("featurefetcher", {})

