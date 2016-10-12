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

#node_IDs = None

def init_data(db_ctt):
    # CTT GATEWAYS
    mdb.add_gateway(db=db_ctt, gateway_eui='AA555A0008060353', 
                placename='Olavskvartalet', latitude=63.433737, longitude=10.403894)
    mdb.add_gateway(db=db_ctt, gateway_eui='AA555A0008060252', 
                placename='Studentersamfundet', latitude=63.422511, longitude=10.395165,
                country='Norway')

    # CTT NODES
    #global node_IDs
    #node_IDs = ["02032201",
    #            "02032220",
    #            "02032221",
    #            "02032222"]
    #for id in node_IDs:
    #    mdb.add_node(db=db_ctt, node_eui=id, placename='node_'+id)
    nodes = [{'placename': u"Kirkegade/Daemningen", 'city':"Vejle", 'node_label':"VJCTT01", 'devAddress':"0E77EE00", 'latitude':55.707520, 'longitude':9.535856, 'country':"Denmark"},
             {'placename': u"Kiretorvet", 'city':"Vejle", 'node_label':"VJCTT02", 'devAddress':"8DEC044C", 'latitude':55.707763, 'longitude':9.532931, 'country':"Denmark"},
             {'placename': u"Vejle Bibliotek", 'city':"Vejle", 'node_label':"VJCTT03", 'devAddress':"C8809DA3", 'latitude':55.705401, 'longitude':9.521222, 'country':"Denmark"},
             {'placename': u"Solsiden Tunnel Apning", 'city':"Trondheim", 'node_label':"TKCTT01", 'devAddress':"AD6AA33E", 'latitude':63.437391, 'longitude':10.415057, 'country':"Norway"},
             {'placename': u"NSB Sentralstasjon", 'city':"Trondheim", 'node_label':"TKCTT02", 'devAddress':"48524DD8", 'latitude':63.435868, 'longitude':10.400028, 'country':"Norway"},
             {'placename': u"Prinsens gate/Kongens gate", 'city':"Trondheim", 'node_label':"TKCTT03", 'devAddress':"E935A419", 'latitude':63.430656, 'longitude':10.392226, 'country':"Norway"},
             {'placename': u"Torvet", 'city':"Trondheim", 'node_label':"TKCTT04", 'devAddress':"7CA37D4E", 'latitude':63.430605, 'longitude':10.396176, 'country':"Norway"},
             #placename: u"Marinen", city:"Trondheim", node_label:"TKCTT05	?	?, country:"Norway"	?},
             #{placename: u"St. Olav Hospital", city:"Trondheim", node_label:"TKCTT06", devAddress:"??", latitude:"63.420500", longitude:"10.387693", country:"Norway"},
             #{placename: u"Elgeseter Gate", city:"Trondheim", node_label:"TKCTT07", devAddress:"2032201", latitude:"63.419287", longitude:"10.396078", country:"Norway"},
             {'placename': u"Hestehagen", 'city':"Trondheim", 'node_label':"TKCTT08", 'devAddress':"9981CAA5", 'latitude':63.415525, 'longitude':10.400920, 'country':"Norway"},
             {'placename': u"Byporten", 'city':"Trondheim", 'node_label':"TKCTT09", 'devAddress':"CD3BE279", 'latitude':63.412797, 'longitude':10.399209, 'country':"Norway"},
             #{placename: u"Solsiden Innherredsvei", city:"Trondheim", node_label:"TKCTT11", devAddress:"????", latitude:"63.433135", longitude:"10.411011", country:"Norway"},
             ##{placename: u"Olav Trygvason/Kjøpmannsgata", city:"Trondheim", node_label:"TKCTT12", devAddress:"???", latitude:"63.432900", longitude:"10.404099", country:"Norway"},
             {'placename': u"Omkjoringsveien", 'city':"Trondheim", 'node_label':"TKCTT10", 'devAddress':"031F5B033", 'latitude':63.403565, 'longitude':10.411040, 'country':"Norway"}]
    
    for nd in nodes:
        nd_eui = nd['devAddress'].zfill(16)
        mdb.add_node(db=db_ctt, node_eui=nd_eui, node_label=nd['node_label'], placename=nd['placename'],
                     latitude=nd['latitude'], longitude=nd['longitude'],
                     city=nd['city'], country=nd['country'])



    # nodeChange={'location_id':5, 'node_eui':"02032201", 'datarate':"SF4334"}
    # update_entry(db=db_ctt, tableName="nodes", entryDict=nodeChange,
    #              whereKey="node_eui", commit=True)

    # CTT topics per "application" i.e. cities in this case (Vejle, Trondheim, ...)
    global applications
    global application
    applications = []
    application = {'applicationName':'CTT_Vejle',
                   'brokerHost':'staging.thethingsnetwork.org',
                   'brokerPort':1883,
                   'appEUI':'70B3D57ED00006CE',
                   'accessKey':'DmaWeq91GIXyqbOWWivU4FEvskLQW1zxdSVt5zy9260='}
    applications.append({'applicationName':'CTT_Vejle',
                         'brokerHost':'staging.thethingsnetwork.org',
                         'brokerPort':1883,
                         'appEUI':'70B3D57ED00006CE',
                         'accessKey':'DmaWeq91GIXyqbOWWivU4FEvskLQW1zxdSVt5zy9260='})
    applications.append({'applicationName':'CTT_Trondheim',
                         'brokerHost':'staging.thethingsnetwork.org',
                         'brokerPort':1883,
                         'appEUI':'70B3D57ED0000785',
                         'accessKey':'xU/EcEgbwysdjQdQPpzzfwuip9IyJQPBFiqenTksJ88='})
    applications.append({'applicationName':'CTT_Trondheim_Deployment',
                         'brokerHost':'staging.thethingsnetwork.org',
                         'brokerPort':1883,
                         'appEUI':'70B3D57ED0000AD8',
                         'accessKey':'LJtFqN8NSqHQzDaaZkHVQ+G+KCDJ+fZbptl94NyUXGg='})

def run():
    nameClientID="VEJLE"
    # Prepare the database
    ## connect to DB
    db_ctt = mdb.open_connection()
    
    ## backup DB if exists
    mdb.backup_DB(backup_path="/tmp/", name=nameClientID)

    ## create tables if missing
    #mdb.drop_CTT_tables(db_ctt)
    #db_ctt.commit()
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
    #topics = ["nodes/"+nID+"/packets" for nID in node_IDs]
    topics = ["70B3D57ED00006CE/devices/+/up"]
    mqtt.set_topics(topics)
    mqtt.set_db(db_ctt)
    mqtt.ctt_collect_MQTT_msg(nameClientID=nameClientID)
    
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
