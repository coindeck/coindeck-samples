import coinlib
import coinlib as c
from coinlib.broker.BrokerDTO import BrokerSymbol
from coinlib.logics.LogicJob import LogicJob
import time

#version: 1.0.0

c.connectAsLogic()

def myTraderLogicCode(inputs, c: LogicJob):
    """
    Add your sourcecode for your trader inside of here.
    """
    return True

c.logic.registerTrader(myTraderLogicCode,
                        "myTraderLogicCode",
                        "This is my Code for Trading.")

### add the logics direct to the workspace
c.addLogicToWorkspace("sl1", "trader", "myTraderLogicCode", workspaceId="{#workspaceId}")


c.waitForJobs()