#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    a script to collect sensor data from The-Things-Network REST API
    ~~~~~~~~
    :copyright: (c) 2016 by Patrick Merlot.
"""

import os, sys, re
import getopt, errno  
import getpass    
import requests
import json
from requests.auth import HTTPBasicAuth
from pprint import pprint
from datetime import datetime
import csv
from collections import deque
import CTT_Nodes

def _url(node, offset=0, limit=250):
    """ A function to simplify building URLs
    e.g. https://www.thethingsnetwork.org/api/v0/nodes/02032201/?limit=250&offset=8000
    """
    url =  'https://www.thethingsnetwork.org/api/v0/nodes/' + \
           node + '/?limit=' + str(limit) + '&offset=' + str(offset)
    print "URL: ",url
    return url


def get_messages(node, offset=0, limit=250):
    """
     This function fetches messages by HTTP request to the REST API
     and returns data in JSON format
    """
    r = requests.get(_url(node, offset, limit))
    jsn = r.json()
    return jsn

        
def get_all_messages(node):
    messages = []
    messages_json = []
    offset = 0 # index at zero pointing to the latest node's/gateway's message stored
    limit = 250
    if limit > 250:
        limit = 250
    res = get_messages(node, offset, limit)
    while len(res) > 0:
        messages.extend(res)
        offset += limit
        res = get_messages(node, offset, limit)

    for msg in messages:
        msg['data_decoded'] = CTT_Nodes.decode_msg_payload(str(msg['data']))
        messages_json.append(msg)
    return messages_json


def main():
    nodes = ["02032220", "02032221", "02032222", "02032201"]
    messages = []
    for node in nodes:
        msg = get_all_messages(node)
        messages.extend(msg)
    pprint(messages)


if __name__ == "__main__": 
    main()

