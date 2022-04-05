import json

import coinlib
import coinlib as c
from coinlib.logics.LogicJob import LogicJob

import pandas as pd
import talib
import plotly.express as px
import numpy as np
import plotly.graph_objects as go

from coinlib.statistics import StatisticMethodJob

from coinlib.helper import pip_install_or_ignore

#version: 1.0.0

pip_install_or_ignore("slack_sdk", "slack_sdk", "3.15.2")

import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

c.connectCoinlib()

def onSlackNotification(inputs, c):

    client = WebClient(token=inputs["bot_token"])
    message = inputs["message"]
    images = inputs["images"] if "images" in inputs else None
    callback_id = inputs["callback_id"] if "callback_id" in inputs else None
    callback_url = inputs["callback_url"] if "callback_url" in inputs else None
    buttons = inputs["buttons"] if "buttons" in inputs else None
    # buttons is type of:
    # {"title": "Button", "id": "callback_id"}

    try:

    except SlackApiError as e:
        raise Exception(json.dumps(e.response["error"]))

    pass


c.notifications.registerNotification(onSlackNotification, "your-slack-notification",
                                     "Slack Notification", [
                                         {"id": "bot_token", "name": "bot_token", "type": "text"}
                                     ], isInteractive=True)

c.simulator.testNotification("slack-notification", {
    "bot_token": "${your_bot_token}"
})


c.waitForJobs()