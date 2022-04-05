import coinlib
import coinlib as c
from coinlib.broker.BrokerDTO import BrokerSymbol
from coinlib.logics.LogicJob import LogicJob
import time

#version: 1.0.0
c.connectAsLogic()

def myScreenerLogicCode(inputs, c: LogicJob):
    """
    Add your sourcecode for your screener inside of here.
    """
    c.log("This is the call inside of your screener")
    return True

c.logic.registerScreener(myScreenerLogicCode,
                        "mySCreenerTest",
                        "This is my Code for Trading.")
### add the logics direct to the workspace
c.addLogicToWorkspace("sl1", "screener", "mySCreenerTest", workspaceId="{#workspaceId}")
c.waitForJobs()
