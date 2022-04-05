#version 1.0.0
import coinlib
import coinlib as c
from coinlib.logics.LogicJob import LogicJob

import pandas as pd
import talib
import plotly.express as px
import numpy as np
import plotly.graph_objects as go

from coinlib.statistics import StatisticMethodJob

c.connect()

def renderTradingMoments(inputs, c: StatisticMethodJob):

    p = c.get("result.logics.trader.buy")
    p = p[p != np.array(None)]
    p2 = c.get("result.logics.trader.sell")
    p2 = p2[p2 != np.array(None)]
    labels = ['Buy Moments', "Sell Moments"]
    values = [len(p),len(p2)]



    fig = go.Figure(data=[go.Pie(labels=labels, textinfo='value', values=values, title="All Tradings")])

    c.plot(fig)

    return True

c.statistics.registerStatisticMethod(renderTradingMoments, "Chart.renderTrading", "Render Trades (Sample)", "", [])

c.simulator.statsMethod([{"method": "Chart.renderTrading", "params": {}}])

c.waitForJobs()