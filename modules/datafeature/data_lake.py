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
from coinlib.feature.CoinlibFeatureLake import CoinlibFeatureLake
from coinlib.feature.CoinlibFeatureProcessor import CoinlibFeatureProcessor
from coinlib.feature.FeatureDTO import FeatureData, ProcessResponse, FeatureLakeData
from coinlib.feature.Features import FeatureModuleType

import requests
import pandas as pd
import coinlib as c
import websockets
from coinlib.logics.LogicJob import LogicJob
import time

##version: 1.0.0

c.connectAsFeature()


class DataFeatureLakeSample(CoinlibFeatureLake):

    def __init__(self):
        super().__init__()
        pass


    async def get_feature_infos(self) -> List[FeatureData]:
        list: List[FeatureData] = [
            FeatureData.create("example_id", "DataFeauture Lake example",  description="Rank inside of Your lake",
                               symbol=True, group="example"),
        ]

        return list

    async def data_received(self, data: FeatureLakeData) -> ProcessResponse:
        p = ProcessResponse()

        self.write_data("example_id", data["example_data"], group="example", interval=60*1000)

        return p


c.features.registerFeature(DataFeatureLakeSample, "featurelake", "Featurelake Example",
                           estimatedDataInterval=24*60*60,
                           type=FeatureModuleType.lake,
                           options=[])


c.simulator.simulateFeature("featurelake", {})

