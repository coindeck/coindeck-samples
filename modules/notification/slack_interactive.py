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

pip_install_or_ignore("slack_sdk", "slack_sdk", "3.15.2")

import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

#version: 1.0.0

c.connectCoinlib()

def onSlackNotification(inputs, c):

    client = WebClient(token=inputs["bot_token"])
    target_channel = "indicators"
    try:

        resp = client.conversations_list()

        channels = resp.data["channels"]
        selected_channel_id = None
        for c in channels:
            if c["name"] == target_channel:
                selected_channel_id = c["id"]

        response = client.conversations_join(channel=selected_channel_id)
        response = client.chat_postMessage(
            channel=selected_channel_id,
            text="Hello from your app! :tada:",
            attachments=[
            {
                "text": "Choose a game to play",
                "fallback": "You are unable to choose a game",
                "callback_id": "wopr_game",
                "image_url": "http://i.imgur.com/OJkaVOI.jpg?1",
                "color": "#3AA3E3",
                "attachment_type": "default",
                "actions": [
                    {
                        "name": "game",
                        "text": "Chess",
                        "type": "button",
                        "value": "chess"
                    },
                    {
                        "name": "game",
                        "text": "Falken's Maze",
                        "type": "button",
                        "value": "maze"
                    },
                    {
                        "name": "game",
                        "text": "Thermonuclear War",
                        "style": "danger",
                        "type": "button",
                        "value": "war",
                        "confirm": {
                            "title": "Are you sure?",
                            "text": "Wouldn't you prefer a good game of chess?",
                            "ok_text": "Yes",
                            "dismiss_text": "No"
                        }
                    }
                ]
            }
        ]
        )
    except SlackApiError as e:
        raise Exception(json.dumps(e.response["error"]))

    pass


c.notifications.registerNotification(onSlackNotification, "slack-notification",
                                     "Slack Notification", [
                                         {"id": "bot_token", "name": "bot_token", "type": "text"}
                                     ])

c.simulator.testNotification("slack-notification", {
    "bot_token": "${your_bot_token}"
})


c.waitForJobs()