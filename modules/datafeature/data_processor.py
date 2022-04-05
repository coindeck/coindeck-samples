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
from coinlib.feature.CoinlibFeatureProcessor import CoinlibFeatureProcessor
from coinlib.feature.FeatureDTO import FeatureData, ProcessResponse

pip_install_or_ignore("coinmarketcapapi", "python-coinmarketcap", "0.3")

from coinmarketcapapi import CoinMarketCapAPI, CoinMarketCapAPIError

import requests
import pandas as pd
import coinlib as c
import websockets
from coinlib.logics.LogicJob import LogicJob
import time

##version: 1.0.0

c.connectAsFeature()


class CoinmarketCap(CoinlibFeatureProcessor):

    def __init__(self):
        super().__init__()
        pass

    async def get_feature_infos(self) -> List[FeatureData]:
        list: List[FeatureData] = [
            FeatureData.create("cmc_rank", "Coinmarketcap Rank",  description="Rank inside of Coinmarketcap",
                               symbol=True, group="cryptostats"),
        ]

        return list

    async def fetchNextDataBlock(self):
        cmc = CoinMarketCapAPI(self.getOption("api_key"))

        transaction = self.transaction()
        r = cmc.cryptocurrency_map(sort="cmc_rank")
        for symbol in r.data:
          self.write_data("cmc_rank", symbol["rank"], group="cryptostats", symbol=symbol["symbol"], transaction=transaction)

        self.finish_transaction(transaction)

        return True

    async def run_process(self) -> ProcessResponse:
        p = ProcessResponse()

        self.timer(30, "fetchData", self.fetchNextDataBlock)

        return p

c.features.registerFeature(CoinmarketCap, "coinmarketcap", "Coinmarketcap.com",
                           estimatedDataInterval=24*60*60,
                           options=[{"type": "text", "name": "api_key"},
                           {"type": "password", "name": "secret"}])


c.simulator.simulateFeature("coinmarketcap", {"api_key": '#{api_key}', "secret": "asdf"})

