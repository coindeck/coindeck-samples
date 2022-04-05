import asyncio
import datetime
import json
import threading
from typing import List

from coinlib import pip_install_or_ignore
from coinlib.broker.Broker import BrokerContractType, BrokerAssetType
from coinlib.broker.BrokerDTO import OpenOrderInfo, CoinlibBrokerLoginParams, AssetInfoSummary, Balance, OpenOrders, \
    Orders, CancelOrder, BrokerSymbol, BrokerTickerData, \
    BrokerDetailedInfo, OrderTypes, OrderSide, OrderUpdateInformation, PositionOrderInfo, \
    TakeProfitStopLoss, PositionList, LeverageUpdateInfo, SelectedSymbolInfo
from coinlib.broker.CoinlibBroker import CoinlibBroker, CoinlibBrokerWatcherThread
from coinlib.broker.CoinlibBrokerFuture import CoinlibBrokerFuture
from coinlib.broker.CoinlibBrokerMargin import CoinlibBrokerMargin
from coinlib.broker.CoinlibBrokerSpot import CoinlibBrokerSpot

pip_install_or_ignore("ccxtpro", "ccxtpro", "1.0.1")

import ccxtpro
import coinlib as c


#version: 1.0.8


def from_order_set_info(info, trade):
    try:
        info.orderId = trade["id"]
        info.timestamp = trade["timestamp"]
        info.price = trade["price"]
        info.averagePrice = trade["average"]
        info.status = trade["status"]
        info.cost = trade["cost"]
        info.remaining = trade["remaining"]
        if "clientOrderId" in trade:
            info.clientOrderId = trade["clientOrderId"]
        info.filled = trade["filled"]
        if "fee" in trade and trade["fee"] is not None and "cost" in trade["fee"]:
            info.fee = trade["fee"]["cost"] if trade["fee"] is not None else 0
        if "fee" in trade and trade["fee"] is not None and "currency" in trade["fee"]:
            info.feeCurrency = trade["fee"]["currency"]
        if "trades" in trade and trade["trades"] is not None:
            info.tradesCnt = len(trade["trades"])
    except Exception as e:
        print("error", e)
        pass
    return info


def reconvertContractType(symbol):
    if symbol == BrokerContractType.FUTURE:
        return "future"
    if symbol == BrokerContractType.MARGIN:
        return "margin"

    return "spot"

def convertContractTypeByInfo(info):
    if "future" == info:
        return BrokerContractType.FUTURE
    if "margin" == info:
        return BrokerContractType.MARGIN

    return BrokerContractType.SPOT

def convertContractType(symbol):
    if "future" in symbol and symbol["future"]:
        return BrokerContractType.FUTURE
    if "margin" in symbol and symbol["margin"]:
        return BrokerContractType.MARGIN

    return BrokerContractType.SPOT

def generateExchange(exchangeid, type, apikey, secret, loop, sandbox=False):
    str_type = reconvertContractType(type)
    if type != BrokerContractType.SPOT:
        options = {
                    'warnOnFetchOpenOrdersWithoutSymbol': False,
                    'defaultType': str_type,
                }
        if type == BrokerContractType.FUTURE:
            options["hedgeMode"] = True
        exchange = getattr(ccxtpro, exchangeid)({
            'enableRateLimit': True,
            'apiKey': apikey,
            'secret': secret,
            'asyncio_loop': loop,
            'options': options})


    else:
        exchange = getattr(ccxtpro, exchangeid)({
            'apiKey': apikey,
            'secret': secret,
            'asyncio_loop': loop,
            'options': {
                'warnOnFetchOpenOrdersWithoutSymbol': False,
            }
        })

    exchange.set_sandbox_mode(sandbox)
    return exchange

c.connectAsBroker()

class CCXTBrokerBasic(CoinlibBroker):


    async def on_start_session(self, is_restart: bool):

        pass

    async def get_broker_info(self) -> BrokerDetailedInfo:
        d = BrokerDetailedInfo()
        d.contractType = convertContractTypeByInfo(self.exchange.options["defaultType"])
        d.makerFee = self.exchange.fees["trading"]["maker"]
        d.takerFee = self.exchange.fees["trading"]["taker"]

        if d.contractType == BrokerContractType.SPOT:
            d.orderTypes = [OrderTypes.LIMIT, OrderTypes.MARKET, OrderTypes.STOP_LOSS, OrderTypes.STOP_LOSS_LIMIT,
                            OrderTypes.TAKE_PROFIT, OrderTypes.TAKE_PROFIT_LIMIT]
            d.canLeverage = False
        elif d.contractType == BrokerContractType.MARGIN:
            d.canLeverage  = True
            d.orderTypes = [OrderTypes.LIMIT, OrderTypes.MARKET, OrderTypes.STOP_LOSS, OrderTypes.STOP_LOSS_LIMIT,
                            OrderTypes.TAKE_PROFIT, OrderTypes.TAKE_PROFIT_LIMIT]
            d.leverageMax = 10
        elif d.contractType == BrokerContractType.FUTURE:
            d.canLeverage = True
            d.orderTypes = [OrderTypes.LIMIT, OrderTypes.MARKET, OrderTypes.STOP, OrderTypes.STOP_MARKET,
                            OrderTypes.TAKE_PROFIT, OrderTypes.TAKE_PROFIT_MARKET, OrderTypes.TRAILING_STOP_MARKET]
            d.leverageMax = 25



        return d

    def runOrdersThreaded(self, broker):
        if len(self.listOfOrders) > 0:
            trade = self.listOfOrders[0]
            self.listOfOrders.remove(trade)
            since_last = trade["timestamp"]
            clientOrderid = trade["clientOrderId"]
            if self.is_order_from_session(clientOrderid):
                info = OrderUpdateInformation()
                info.clientOrderId = clientOrderid
                info = from_order_set_info(info, trade)
                broker.sendOrderUpdate(info)

            listenThread = threading.Thread(target=self.runOrdersThreaded, args=[broker], daemon=True)
            listenThread.start()

    async def watch_orders_live(self, broker, myTask):

        exchange = broker.generate_exchange(myTask.get_loop())
        try:
            since_last = 0
            while myTask.running():
                ## filter all threads
                ##task = myTask.run_task(exchange.watch_orders(since=0))
                ##all_trades = await task
                all_trades = await exchange.watch_orders()
                if myTask.running():
                    last_symbols = []
                    for trade in reversed(all_trades):
                        self.listOfOrders.append(trade)
                    listenThread = threading.Thread(target=self.runOrdersThreaded, args=[broker], daemon = True)
                    listenThread.start()


        except asyncio.CancelledError as ce:
            print("cancelled error", ce)
            pass
        await exchange.close()

    async def login(self, login_mode: str, login_params: CoinlibBrokerLoginParams):
        self.exchange = self.generate_exchange()

        p = await self.exchange.fetch_balance()

        self.register_task("watch_orders_task", self.watch_orders_live, singleton=True)
        pass

    def generate_exchange(self, loop=None):
        return generateExchange(self.account.exchange_id, self.account.contractType,
                                         self.account.loginInfo.apiKey, self.account.loginInfo.secret,
                                         loop if loop is not None else self.get_loop(),
                                         sandbox=self.account.sandbox)

    async def shutdown(self):
        await self.exchange.close()

    async def get_assets(self) -> AssetInfoSummary:
        pass


    async def get_price(self, base, quote) -> float:
        try:
            symbol = base+"/"+quote
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe="1m", limit=1)
            return ohlcv[0][4]
        except Exception as e:
            pass
        return 0

    async def watch_price(self, symbol_id, workerThread: CoinlibBrokerWatcherThread):

        nexchange = workerThread.get_broker().generate_exchange(workerThread.get_loop())
        last_price = await nexchange.fetch_ohlcv(symbol_id, timeframe="1m", limit=1)
        ticker = BrokerTickerData()
        ticker.open = last_price[0][1]
        ticker.high = last_price[0][2]
        ticker.low = last_price[0][3]
        ticker.close = last_price[0][4]
        ticker.volume = last_price[0][5]
        tickerTimestamp = datetime.datetime.fromtimestamp(last_price[0][0] / 1000)
        ticker.date = tickerTimestamp
        ticker.symbol_id = symbol_id
        workerThread.sendTicker(ticker)
        try:
            while workerThread.running():
                try:
                    my_task = workerThread.run_task(nexchange.watch_ticker(symbol_id))
                    tickerData = await my_task
                    if workerThread.running():
                        ticker = BrokerTickerData()
                        ticker.open = tickerData["open"]
                        ticker.high = tickerData["high"]
                        ticker.low = tickerData["low"]
                        ticker.close = tickerData["close"]
                        ticker.volume = tickerData["baseVolume"]
                        ticker.date = tickerData["datetime"]
                        ticker.symbol_id = symbol_id
                        workerThread.sendTicker(ticker)
                except Exception as e:
                    pass
        except asyncio.CancelledError as ce:
            pass

        await nexchange.close()


    async def watch_price_ticker(self, symbol_id: str)  -> SelectedSymbolInfo:

        self.register_price_ticker(self.watch_price, symbol_id)

        info = SelectedSymbolInfo()
        info.canLeverage = True

        try:
            symbol = self.exchange.market(symbol_id)
            leverageBrackets = await self.exchange.fapiPrivateGetLeverageBracket({  # https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods
                'symbol': symbol["id"],
            })
            positionRisk = await self.exchange.fapiPrivateGetPositionRisk({  # https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods
                'symbol': symbol["id"],
            })

            info.leverageMax = leverageBrackets[0]["brackets"][0]["initialLeverage"]
            info.currentLeverage = positionRisk[0]["leverage"]
        except Exception as e:
            pass



        return info

    async def get_symbols(self) -> List[BrokerSymbol]:
        list = []
        symbols = await self.exchange.load_markets()
        symbol_json = json.dumps(symbols)
        obj = json.loads(json.dumps(symbols))
        already_found = {}
        for s in obj:
            symbol = symbols[s]

            ne = BrokerSymbol()
            ne.client_id = symbol["symbol"]
            ne.symbol = symbol["symbol"]
            ne.base = symbol["base"]
            ne.quote = symbol["quote"]
            ne.price_precision = symbol["precision"]["price"]
            ne.size_precision = symbol["precision"]["amount"]
            ne.symbol = symbol["symbol"]

            if ne.client_id not in already_found:
                list.append(ne)
                already_found[ne.client_id] = ne

        return list

    async def get_balance(self) -> Balance:
        balance = Balance()

        b = await self.exchange.fetch_balance()
        for assetName in b:
            try:
                asset = b[assetName]
                if "free" in asset:
                    price = 0
                    if asset["total"] > 0:
                        price = await self.get_price(assetName, self.get_basic_quote_asset_name())
                    balance.addAsset(assetName, asset["free"], asset["used"], asset["total"], price)
            except Exception as e:
                pass

        return balance

    async def get_open_orders(self) -> OpenOrders:
        all_trades = await self.exchange.fetch_open_orders()
        orders = []
        for trade in all_trades:
            info = OrderUpdateInformation()
            info.clientOrderId = trade["clientOrderId"]
            info = from_order_set_info(info, trade)
            orders.append(info)

        return orders

    async def get_order_detail(self, order_id: str, clientOrderId: str, symbol_id: str):
        trade = await self.exchange.fetch_order(id=order_id, symbol=symbol_id)
        info = OrderUpdateInformation()
        if "clientOrderId" in trade:
            info.clientOrderId = trade["clientOrderId"]
        info = from_order_set_info(info, trade)

        return info


    async def get_orders(self, symbol_id) -> Orders:
        all_trades = await self.exchange.fetch_my_trades(symbol_id)
        orders = []
        for trade in all_trades:
            info = OrderUpdateInformation()
            if "clientOrderId" in trade:
                info.clientOrderId = trade["clientOrderId"]
            info = from_order_set_info(info, trade)
            orders.append(info)

        return orders

    async def cancel_order(self, order_id: str, clientOrderId: str, symbol_id: str) -> CancelOrder:

        try:
            info = CancelOrder()
            order = await self.exchange.cancel_order(order_id, symbol_id)

            info = from_order_set_info(info, order)
            info.clientOrderId = clientOrderId
        except Exception as e:
            pass

        return info


class BinanceBrokerSpot(CCXTBrokerBasic, CoinlibBrokerSpot):
    async def create_order_stop_limit(self, asset: str, amount: float, limit_price: float, stop_price: float,
                                      side: OrderSide = OrderSide.buy, clientOrderId: str = None) -> OpenOrderInfo:
        pass

    async def create_order_oco(self, asset: str, amount: float, price: float, stop_price: float, limit_price: float,
                               side: OrderSide = OrderSide.buy, clientOrderId: str = None) -> OpenOrderInfo:
        pass

    async def create_order_market(self, asset: str, amount: float,
                                  side: OrderSide = OrderSide.buy, clientOrderId: str = None) -> OpenOrderInfo:
        info = OpenOrderInfo()
        order = await self.exchange.create_order(asset, "market", side, amount, params={"clientOrderId": clientOrderId})

        info = from_order_set_info(info, order)
        info.clientOrderId = clientOrderId

        self.update_order(info)

        return info

    async def create_order_limit(self, asset: str, amount: float, limit_price: float,
                                 side: OrderSide = OrderSide.buy, clientOrderId: str = None) -> OpenOrderInfo:

        info = OpenOrderInfo()
        order = await self.exchange.create_order(asset, "limit", side, amount, price=limit_price, params={"clientOrderId": clientOrderId})

        info = from_order_set_info(info, order)
        info.clientOrderId = clientOrderId

        self.update_order(info)

        return info


class BinanceBrokerFuture(CCXTBrokerBasic, CoinlibBrokerFuture):

    async def get_positions(self) -> PositionList:
        pass

    async def create_order_stop_limit(self, asset: str, amount: float, limit_price: float, stop_price: float,
                                      leverage: float = 1, clientOrderId: str = None) -> OpenOrderInfo:
        pass

    async def create_order_trailing_stop(self, asset: str, amount: float, activation_price: float,
                                         callback_rate: float = 0.01,
                                         leverage: float = 1, clientOrderId: str = None) -> OpenOrderInfo:
        pass

    async def create_order_market(self, asset: str, amount: float, side: OrderSide = OrderSide.buy,
                                  take_profit_stop_los: TakeProfitStopLoss = None,
                                  leverage: float = 1, clientOrderId: str = None) -> OpenOrderInfo:
        await self.set_leverage(asset, leverage)

        info = OpenOrderInfo()
        position_side = "LONG"
        if side == "buy":
            position_side = "LONG"
        else:
            position_side = "SHORT"
        order = await self.exchange.create_order(asset, "market", side, amount,
                                                 params={"clientOrderId": clientOrderId,
                                                         "positionSide": position_side})

        info = from_order_set_info(info, order)
        info.clientOrderId = clientOrderId

        self.update_order(info)

        return info

    async def create_order_limit(self, asset: str, amount: float, limit_price: float,
                                 side: OrderSide = OrderSide.buy, take_profit_stop_los: TakeProfitStopLoss = None,
                                 clientOrderId: str = None,
                                 leverage: float = 1) -> OpenOrderInfo:

        await self.set_leverage(asset, leverage)

        info = OpenOrderInfo()
        position_side = "LONG"
        if side == "buy":
            position_side = "LONG"
        else:
            position_side = "SHORT"
        order = await self.exchange.create_order(asset, "limit", side, amount,
                                                 price=limit_price,
                                                 params={"clientOrderId": clientOrderId,
                                                         "positionSide": position_side})

        info = from_order_set_info(info, order)
        info.clientOrderId = clientOrderId

        self.update_order(info)

        return info

    async def close_position_market(self, asset:str, position_order_id: str, amount: float, side: str, positionSide: str,
                                    clientOrderId=None) -> PositionOrderInfo:

        order = await self.exchange.create_order(asset, "market", side.upper(), amount,
                                                 params={"clientOrderId": clientOrderId,
                                                         "positionSide": positionSide.upper()})

        info = OpenOrderInfo()
        info = from_order_set_info(info, order)
        info.clientOrderId = clientOrderId

        self.update_order(info)

        return info

    async def close_position_limit(self, asset: str, position_order_id: str,  amount: float, limit_price: float, positionSide: str,
                                 side: OrderSide = OrderSide.buy,  take_profit_stop_los: TakeProfitStopLoss = None,
                                 clientOrderId: str = None) -> PositionOrderInfo:
        pass

    async def set_leverage(self, symbol_id: str, leverage: float) -> LeverageUpdateInfo:

        symbol = self.exchange.market(symbol_id)
        await self.exchange.fapiPrivatePostLeverage({  # https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods
            'symbol': symbol["id"],
            'leverage': leverage,  # target initial leverage, int from 1 to 125
        })

        info = LeverageUpdateInfo()
        info.setLeverage = leverage

        return info

class BinanceBrokerMargin(CCXTBrokerBasic, CoinlibBrokerMargin):
    pass




supported_exchanges = ["binance"]
all_exchanges = []


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
for exchange_name in supported_exchanges:
    contractTypes = []
    exchange = getattr(ccxtpro, exchange_name)()

    if "typesByAccount" in exchange.options:
        for ktype in exchange.options["typesByAccount"]:
            type = exchange.options["typesByAccount"][ktype]
            if type == "future":
                contractTypes.append(BrokerContractType.FUTURE)
            if type == "spot":
                contractTypes.append(BrokerContractType.SPOT)
            if type == "margin":
                contractTypes.append(BrokerContractType.MARGIN)
    else:
        contractTypes.append(BrokerContractType.SPOT)


    all_exchanges.append({"key": exchange.id, "name": exchange.name,
                           "contractTypes": contractTypes,
                          "positionModeFuture": "merge",
                           "logo": exchange.urls["logo"],
                          "supportDemo": True})

c.broker.registerBroker("binance", "binance",
                        all_exchanges,
                        brokerSpotClass=BinanceBrokerSpot,
                        brokerFutureClass=BinanceBrokerFuture,
                        brokerMarginClass=BinanceBrokerMargin)


c.simulator.testBroker("binance", {"exchange_id": "binance", "loginInfo":
                                    {"apikey": "#{your_api_key}",
                                     "secret": "#{your_api_secret}"},
                                "assetType": BrokerAssetType.COIN,
                                "paperMode": True,
                                "contractType": BrokerContractType.SPOT
                                })


c.waitForJobs()