import asyncio
import datetime
import json
import threading
import dateutil.parser as dp
import dateutil
import dateutil.parser
import calendar

import pytz

from coinlib import pip_install_or_ignore
from coinlib.dataWorker_pb2 import SymbolBrokerExchange, SymbolBrokerSymbol, SymbolBrokerMarketData
from coinlib.helper import isFinalTimestamp, periodInSeconds, createTimerWithTimestamp
from coinlib.symbols import SymbolWorker
import coinlib as c
from coinlib.logics.LogicJob import LogicJob
from coinlib.symbols.SymbolWorker import ExchangeInfoRequest, HistoricalSymbolRequest, OrderBookConsumer, \
    MarketDataConsumer, SymbolWorkerFetchHistoricalData, SymbolWorkerConsumeMarketData, SymbolBrokerTickerSymbolData


pip_install_or_ignore("ccxtpro", "ccxtpro", "1.0.1")


import requests
import pandas as pd
import ccxtpro
import coinlib as c
from coinlib.logics.LogicJob import LogicJob
import time


clb = c

##version: 1.4.12

c.connectAsBrokerSymbol("test2.coin-deck.de:9009")

def convertTimeframesBack(coindeckTf):
    if coindeckTf.endswith("SEC"):
        return coindeckTf.replace("SEC", "s")
    if coindeckTf.endswith("HRS"):
        return coindeckTf.replace("HRS", "h")
    if coindeckTf.endswith("MIN"):
        return coindeckTf.replace("MIN", "m")
    if coindeckTf.endswith("DAY"):
        return coindeckTf.replace("DAY", "d")
    if coindeckTf.endswith("MTH"):
        return coindeckTf.replace("MTH", "M")
    if coindeckTf.endswith("YRS"):
        return coindeckTf.replace("YRS", "Y")

    return coindeckTf

def convertTimeframes(coindeckTf):
    if coindeckTf.endswith("s"):
        return coindeckTf.replace("s", "SEC")
    if coindeckTf.endswith("h"):
        return coindeckTf.replace("h", "HRS")
    if coindeckTf.endswith("m"):
        return coindeckTf.replace("m", "MIN")
    if coindeckTf.endswith("d"):
        return coindeckTf.replace("d", "DAY")
    if coindeckTf.endswith("M"):
        return coindeckTf.replace("M", "MTH")
    if coindeckTf.endswith("Y"):
        return coindeckTf.replace("Y", "YRS")
    if coindeckTf.endswith("y"):
        return coindeckTf.replace("y", "YRS")

    return coindeckTf

def generateExchange(exchangeid, type, apikey, secret, ccxt_or_ccxt_async=ccxtpro):

    if type == SymbolBrokerSymbol.ContractType.FUTURE:
        exchange = getattr(ccxtpro, exchangeid)({
            'apiKey': apikey,
            'secret': secret,
            'options': {
                    'defaultType': reconvertContractType(type),
                }})
    else:
        exchange = getattr(ccxtpro, exchangeid)({
            'apiKey': apikey,
            'secret': secret,
        })


    return exchange

def extractApiInformation(options, exchangeid, contracttype):
    for o in options:
        if "_apikey" in o:
            val = options[o]
            defaultType = "spot"
            if len(o.split("_")) > 2:
                defaultType = o.split("_")[1]
                if defaultType == "fts":
                    defaultType = "future"
                if defaultType == "mgn":
                    defaultType = "margin"
            exchange_info = {
                "name": o.split("_")[0].strip(),
                "id": o.split("_")[0].strip(),
                "type": defaultType,
                "apikey": val["apikey"],
                "secret": val["secret"]
            }
            if exchange_info["id"] == exchangeid and exchange_info["type"] == contracttype:
                return exchange_info
    return None

async def downloadHistoricalData(historicalReq: HistoricalSymbolRequest, c: SymbolWorkerFetchHistoricalData):

    apiInfo = extractApiInformation(historicalReq.options, historicalReq.exchange_id, reconvertContractType(historicalReq.contractType))
    exchange = generateExchange(historicalReq.exchange_id, historicalReq.contractType, apiInfo["apikey"], apiInfo["secret"])
    ts = datetime.datetime.strptime(historicalReq.start, "%Y-%m-%dT%H:%M:%S.%fZ") # 2021-04-01T19:42:21.176Z
    startTime = calendar.timegm(ts.utctimetuple())
    ts = datetime.datetime.strptime(historicalReq.end, "%Y-%m-%dT%H:%M:%S.%fZ")  # 2021-04-01T19:42:21.176Z
    endTime = calendar.timegm(ts.utctimetuple())
    timestamp = startTime * 1000
    nowTime = int(time.time())
    if endTime > nowTime:
        endTime = nowTime

    minimumSlotTime = periodInSeconds(historicalReq.timeframe)
    endTime = endTime - minimumSlotTime
    endTime = int(endTime * 1000)

    fullData = []
    for i in range(30):
        data = await exchange.fetch_ohlcv(historicalReq.symbol_id, historicalReq.timeframe, since=timestamp, limit=99999)

        if len(data) > 0:
            lastElement = data[-1]
            timestamp = lastElement[0]
            fullData.extend(data)

            check_timestamp_minute = int(timestamp/1000 / 60) * 60 * 1000
            check_end_time = int(endTime / 1000 / 60) * 60 * 1000

            if check_timestamp_minute >= check_end_time:
                break
        else:
            break

    try:
        df = pd.DataFrame(fullData, columns=["datetime", "open", "high", "low", "close", "volume"])


        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df = df.set_index('datetime')

        c.sendDataFrame(df)
    except Exception as e:
        pass

    await exchange.close()

    return None

def reconvertContractType(symbol):
    if symbol == SymbolBrokerSymbol.ContractType.FUTURE:
        return "future"
    if symbol == SymbolBrokerSymbol.ContractType.MARGIN:
        return "margin"

    return "spot"


def convertContractType(symbol):
    if "future" in symbol and symbol["future"]:
        return SymbolBrokerSymbol.ContractType.FUTURE

    return SymbolBrokerSymbol.ContractType.SPOT


async def downloadAllExchangesAndSymbols(req: ExchangeInfoRequest, c: SymbolWorker):
    exchanges = []
    symbols = []

    exchanges_api_keys = []
    for o in req.options:
        if "_apikey" in o:
            val = req.options[o]
            defaultType = "spot"
            if len(o.split("_")) > 2:
                defaultType = o.split("_")[1]
            exchanges_api_keys.append({
                "name": o.split("_")[0].strip(),
                "id": o.split("_")[0].strip(),
                "type": defaultType,
                "apikey": val["apikey"],
                "secret": val["secret"]
            })

    exchanges_dto = []
    symbols_dto = []
    for exchange_id in ccxtpro.exchanges:
        try:
            exchange = getattr(ccxtpro, exchange_id)()
            ne = SymbolBrokerExchange()

            ne.icon = exchange.urls["logo"]
            ne.name = exchange.name
            ne.exchange_id = exchange.id
            #ne.volume_1mth_usd = e["volume_1mth_usd"]
            #ne.volume_1day_usd = e["volume_1day_usd"]
            #ne.volume_1hrs_usd = e["volume_1hrs_usd"]
            ne.website = exchange.urls["www"]
            #ne.symbols_count = e["data_symbols_count"]
            exchanges_dto.append(ne)
        except Exception as e:
            pass
        finally:
            if exchange is not None:
                await exchange.close()


    for exchangeData in exchanges_api_keys:
        try:
            exchange = None
            already_found = {}
            default_type = SymbolBrokerSymbol.ContractType.SPOT
            if "fts" in exchangeData["type"] or "future" in exchangeData["type"]:
                default_type = SymbolBrokerSymbol.ContractType.FUTURE
            if "mgn" in exchangeData["type"] or "margin" in exchangeData["type"]:
                default_type = SymbolBrokerSymbol.ContractType.MARGIN

            if exchangeData["apikey"] is not None and exchangeData["apikey"].strip() != "":
                exchange = generateExchange(exchangeData["id"], default_type, exchangeData["apikey"], exchangeData["secret"])

                symbols = await exchange.load_markets()
                symbol_json = json.dumps(symbols)
                obj = json.loads(json.dumps(symbols))
                for s in obj:
                    symbol = symbols[s]

                    ne = SymbolBrokerSymbol()
                    ne.client_id = symbol["symbol"]
                    ne.symbol = symbol["symbol"]
                    ne.exchange_id = exchangeData["id"]
                    ne.assetType = SymbolBrokerSymbol.AssetType.COIN
                    ne.contractType = convertContractType(symbol)

                    ne.base = symbol["base"]

                    ne.quote = symbol["quote"]
                    ne.exchange_symbol_id = exchangeData["id"]+"_"+symbol["symbol"]
                    ne.price_precision = symbol["precision"]["price"]
                    ne.size_precision = symbol["precision"]["amount"]
                    ne.symbol = symbol["symbol"]

                    if str(ne.contractType)+"_"+ne.exchange_symbol_id not in already_found:
                        symbols_dto.append(ne)
                        already_found[str(ne.contractType)+"_"+ne.exchange_symbol_id] = ne
        except Exception as e:
            pass
        finally:
            if exchange is not None:
                await exchange.close()

    c.saveBrokerInfo(exchanges_dto, symbols_dto)



    return None


##downloadAllExchangesAndSymbols(None, None)


async def consumeOrderbook(reqData: OrderBookConsumer, c: SymbolWorker):
    """
    async with websockets.connect(ws_endpoint) as websocket:

        while not c.canceled() and websocket is not None:

            async for message_str in websocket:

                try:
                    message = json.loads(message_str)

                    if message["type"] == "book20":
                        print(message["price_close"])

                except Exception as e:
                    print(e)

                if c.canceled():
                    break

        websocket.close()
    """

async def getCandles(reqData: MarketDataConsumer,
                     c: SymbolWorkerConsumeMarketData, limit: int) -> [SymbolBrokerTickerSymbolData]:
    apiInfo = extractApiInformation(reqData.options, reqData.exchange_id, reconvertContractType(reqData.contractType))
    exchange = generateExchange(reqData.exchange_id, reqData.contractType, apiInfo["apikey"], apiInfo["secret"])

    allticker = await exchange.fetch_ohlcv(reqData.symbol_id, reqData.timeframe, limit=limit)
    tickers = []

    await exchange.close()

    for ticker in allticker:
        t = SymbolBrokerTickerSymbolData()
        t.open = ticker[1]
        t.high = ticker[2]
        timestamp = datetime.datetime.fromtimestamp(ticker[0] / 1000, tz=datetime.timezone.utc)
        t.closeTime = datetime.datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%Z")
        t.datetime = datetime.datetime.strftime(datetime.datetime.now(pytz.utc), "%Y-%m-%dT%H:%M:%S.%f%Z")
        t.low = ticker[3]
        t.close = ticker[4]
        t.volume = ticker[5]
        tickers.append(t)

    return tickers



async def consumeMarketData(reqData: MarketDataConsumer, c: SymbolWorkerConsumeMarketData):

    apiInfo = extractApiInformation(reqData.options, reqData.exchange_id, reconvertContractType(reqData.contractType))
    exchange = generateExchange(reqData.exchange_id, reqData.contractType, apiInfo["apikey"], apiInfo["secret"])
    lastTimestamp = 0
    lastSendData = 0

    while not c.canceled():
        # this can be any call instead of fetch_ticker, really
        try:

            allticker = await exchange.watch_ohlcv(reqData.symbol_id, reqData.timeframe, limit=1)
            ticker = allticker[0]
            timestamp = datetime.datetime.fromtimestamp(ticker[0]/1000, tz=datetime.timezone.utc)
            t = SymbolBrokerTickerSymbolData()
            t.open = ticker[1]
            t.high = ticker[2]
            t.low = ticker[3]
            t.close = ticker[4]
            t.volume = ticker[5]
            t.trades = -1
            t.timeframe = reqData.timeframe
            t.symbol_id = reqData.exchange_id+"_"+reqData.symbol_id
            t.symbol = reqData.symbol_id
            t.isFinal = False
            t.closeTime = datetime.datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%Z")
            t.datetime = datetime.datetime.strftime(datetime.datetime.now(pytz.utc), "%Y-%m-%dT%H:%M:%S.%f%Z")

            lastTimestamp = ticker[0]

            c.sendSymbolTicker(t)

        except ccxtpro.RequestTimeout as e:
            print('[' + type(e).__name__ + ']')
            print(str(e)[0:200])
            # will retry
        except ccxtpro.DDoSProtection as e:
            print('[' + type(e).__name__ + ']')
            print(str(e.args)[0:200])
            # will retry
        except ccxtpro.ExchangeNotAvailable as e:
            print('[' + type(e).__name__ + ']')
            print(str(e.args)[0:200])
            # will retry
        except ccxtpro.ExchangeError as e:
            print('[' + type(e).__name__ + ']')
            print(str(e)[0:200])
            break  # won't retry
        except Exception as e:
            print(e)
            break

    await exchange.close()



c.symbols.registerSymbolBroker(downloadAllExchangesAndSymbols,
                               downloadHistoricalData,
                               consumeMarketData,
                               getCandles,
                               consumeOrderbook, "binance", "binance",
                               [{"id": "binance_apikey", "name": "Binance API Key", "type": "apikey"},
                                {"id": "binance_fts_apikey", "name": "Binance Futures API Key", "type": "apikey"}
                                ])



c.simulator.testSymbolBroker("binance",
                               "BINANCE_BTC_USDT",
                               "BITFINES_ETH_USDT", {"binance_apikey":
                                    {"apikey": "#{your_api_key}",
                                     "secret": "#{your_api_secret}"},
                                                     "binance_fts_apikey":
                                    {"apikey": "#{your_api_key}",
                                     "secret": "#{your_api_secret}"}}
                        )
