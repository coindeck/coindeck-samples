import coinlib as c
from coinlib.broker.BrokerDTO import BrokerSymbol
from coinlib.logics.LogicJob import LogicJob

import time

#version: 1.0.0
c.connectAsLogic()

def myDataLogic(inputs, c: LogicJob):
    """
    Add your sourcecode for your trader inside of here.
    """
    return True

c.logic.registerLogic(myDataLogic,
                        "myDataLogic",
                        "This is my Code for Logic.")

### add the logics direct to the workspace
c.addLogicToWorkspace("sl1", "logic", "myDataLogic", workspaceId="{#workspaceId}")
c.waitForJobs()