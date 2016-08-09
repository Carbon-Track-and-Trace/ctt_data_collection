#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    A collection of function to deal with the nodes, and the data they generates
    ~~~~~~~~
    :copyright: (c) 2016 by Patrick Merlot.
"""

import os, sys, re
import json
from pprint import pprint
from datetime import datetime
import base64 
from collections import deque


def extract_payload(payload):
    """ From a device's payload like this one:
    <=> #403472398#CTT_Module1#144#GP_CO2:255.398#BAT:98#
    this functions returns the following data as dictionary
    # Serial ID: 403472398
    # Waspmote ID: CTT_Module1
    # Sequence: 144
    # sensor1 key/value: GP_CO2/255.398
    # sensor2 key/value: BAT/98
    """
    ###payload = '<=>\x80\x02#403472398#CTT_Module1#55#GP_CO2:205.790#BAT:96#'    
    sp = deque(payload.split("#"))
    prelude = sp.popleft()
    dataDict = {}
    dataDict['serial_id']    = sp.popleft()
    dataDict['waspmote_id']  = sp.popleft()
    dataDict['sequence_Num'] = sp.popleft()
    dataDict['sensors'] = {}
    while len(sp) > 0:
        sensor = sp.popleft()
        if sensor.strip() != '': 
            kv = sensor.split(":")
            dataDict['sensors']['SENSOR_'+kv[0]] = kv[1]
    return dataDict
        
def decode_msg_payload(base64EncodedPayload):
    payload = base64.b64decode(base64EncodedPayload)
    return payload

def main():
    pass

if __name__ == "__main__": 
    main()

