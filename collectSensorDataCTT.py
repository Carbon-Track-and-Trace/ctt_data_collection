#! /usr/bin/env python
# -*- coding: utf-8 -*-

""" 
  Script to prepare the database and collect all sensors' data 
  from the CarbonTrack&Trace project
    ~~~~~~~~
    :copyright: (c) 2016 by Patrick Merlot.
"""

import sys, os
import string
import logging
import pprint
import numpy as np
from datetime import datetime
from dateutil.parser import parse
from collections import defaultdict
from itertools import groupby
from collections import OrderedDict
import json 
import jsonschema
import pytz
from subprocess import call
import paho.mqtt.client as paho

import CTT_Nodes
import CTT_monetdb_API as mdb
import CTT_TTN_REST_API as rest
import CTT_TTN_MQTT_API as mqtt

DEBUG = False

node_IDs = None

def init_data(db_ctt):
    # CTT GATEWAYS
    mdb.add_gateway(db=db_ctt, gateway_eui='AA555A0008060353', 
                placename='Olavskvartalet', latitude=63.433737, longitude=10.403894)
    mdb.add_gateway(db=db_ctt, gateway_eui='AA555A0008060252', 
                placename='Studentersamfundet', latitude=63.422511, longitude=10.395165,
                country='Norway')

    # CTT NODES
    global node_IDs
    node_IDs = ["02032201",
                "02032220",
                "02032221",
                "02032222"]
    for id in node_IDs:
        mdb.add_node(db=db_ctt, node_eui=id, placename='node_'+id)

    # nodeChange={'location_id':5, 'node_eui':"02032201", 'datarate':"SF4334"}
    # update_entry(db=db_ctt, tableName="nodes", entryDict=nodeChange,
    #              whereKey="node_eui", commit=True)

    # CTT topics per "application" i.e. cities in this case (Vejle, Trondheim, ...)
    applications = []
    applications.append({'applicationName':'CTT_Vejle',
                         'brokerHost':'staging.thethingsnetwork.org',
                         'AppEUI':'70B3D57ED00006CE',
                         'AccessKey':'DmaWeq91GIXyqbOWWivU4FEvskLQW1zxdSVt5zy9260='})
    # applications.append({'applicationName':'CTT_Trondheim',
    #                      'brokerHost':'staging.thethingsnetwork.org',
    #                      'AppEUI':'70B3D57ED0000785',
    #                      'AccessKey':'xU/EcEgbwysdjQdQPpzzfwuip9IyJQPBFiqenTksJ88='})
    # applications.append({'applicationName':'CTT_Trondheim_Deployment',
    #                      'brokerHost':'staging.thethingsnetwork.org',
    #                      'AppEUI':'70B3D57ED0000AD8',
    #                      'AccessKey':'LJtFqN8NSqHQzDaaZkHVQ+G+KCDJ+fZbptl94NyUXGg='})

def run():
    # Prepare the database
    ## connect to DB
    db_ctt = mdb.open_connection()
    
    ## backup DB if exists
    #mdb.backup_DB(backup_path="/tmp/")

    ## create tables if missing
    #mdb.drop_CTT_tables(db_ctt)
    db_ctt.commit()
    mdb.create_CTT_tables(db_ctt)
    db_ctt.commit()
    init_data(db_ctt)
    
    # # collect and store historical data for node '02032201' directly from file
    # # wget http://129.241.209.185:1880/api/02032201 ../Data_archives/02032201.json
    # archives = ['../Data_archives/02032201.json',
    #             '/home/patechoc/Documents/CODE-DEV/AIA/Project_Carbon-Track-and-Trace/CTT_dashboard/Data_archives/02032201.json']
    # for archive in archives:
    #     try:
    #         with open(archive) as json_data:
    #             oldMsg = json.load(json_data)
    #             tt = len(oldMsg)
    #             for m, msgMQTT in enumerate(oldMsg):
    #                 print "msg #{m}/{t}".format(m=m, t=tt)
    #                 base64EncodedPayload = CTT_Nodes.decode_msg_payload(msgMQTT['data'])
    #                 payload = CTT_Nodes.extract_payload(base64EncodedPayload)
    #                 msgMQTT.update(payload)
    #                 msgDict = mqtt.map_msg_MQTT_to_monetdb(msgMQTT)
    #                 if DEBUG:
    #                     pprint.pprint(msgDict)
    #                 if db_ctt != None:
    #                     mdb.add_node_message(db=db_ctt, msg=msgDict)
    #                     db_ctt.commit()        
    #             json_data.close()
    #     except IOError:
    #         pass

    # # collect and store historical data from TTN REST API
    # messages = []
    # for node in node_IDs:
    #     msg = rest.get_all_messages(node)
    #     messages.extend(msg)
    # for msg in messages:
    #     msgDict = msg
    #     payload = CTT_Nodes.extract_payload(msg['data_decoded'])
    #     msgDict.update(payload)
    #     #pprint.pprint(msgDict)
    #     mdb.add_node_message(db=db_ctt, msg=msgDict)
    #     db_ctt.commit()        

    # collect and store indefinitely real-time data from TTN MQTT Broker
    # include log/notifications when important changes occur
    #  (new device location, new/lost sensor, new gateway for comm., ... )
    topics = ["nodes/"+nID+"/packets" for nID in node_IDs]
    mqtt.set_topics(topics)
    mqtt.set_db(db_ctt)
    mqtt.ctt_collect_MQTT_msg()
    
    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()


    # close monetdb connection
    db_ctt.commit()
    mdb.close_connection(db_ctt)


if __name__ == "__main__": 
    run()
