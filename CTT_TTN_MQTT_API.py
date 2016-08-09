#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    CTT Data Collection of real-time MQTT messages
    ~~~~~~~~
    :copyright: (c) 2016 by Patrick Merlot.
"""

import paho.mqtt.client as paho
from pprint import pprint
import json
import CTT_monetdb_API as mdb
import CTT_Nodes

topics = None

db = None

def set_topics(tpcs):
    """ This function reads a list of topics
    and make it a global variable
    """
    global topics
    topics = tpcs

def set_db(db_connection):
    """ This function reads the monetdb connection object
    and make it a global variable
    """
    global db
    db = db_connection

def on_connect(client, userdata, flags, rc):
    """
    The callback for when the client receives a CONNACK response from the server.
    """
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    print("\nYou are connected with result code "+str(rc))
    for tpc in topics:
        client.subscribe(topic=tpc, qos=0)
        print("\nYou subscribed to this topic: {topic}".format(topic=tpc))
    # Per topic message callbacks
    client.message_callback_add("nodes/+/packets", on_message_packets)

def on_publish(client, userdata, mid):
    print("Publihsed, mid: " + str(mid))

def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed, mid: " + str(mid) + " " + str(granted_qos))

def on_log(client, userdata, level, string):
    print(string)
    
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Ooops OOops... Unexpected disconnection.")

def map_msg_MQTT_to_monetdb(msgMQTT): 
    """
    simply map the keys obtained from the MQTT message 
    to the fieldname of the monetdb tables
    """
    mapping_MQTT_to_DB = {u'gatewayEui': "gateway_eui",
                          u'nodeEui': "node_eui",
                          u'dataRate': "datarate",
                          u'data': "data_base64",}
    msgDict = msgMQTT
    for oldKey in mapping_MQTT_to_DB.keys():
        newKey = mapping_MQTT_to_DB[oldKey]
        msgDict[newKey] = msgMQTT.pop(oldKey)
    return msgDict
    
   # Expected monetdb dictionary:
    # {'datarate': u'SF7BW125',
    #  'frequency': 868.3,
    #  'gateway_eui': u'0000024B080E06B3',
    #  'node_eui': u'02032222',
    #  'rssi': -103,
    #  'sensor_bat': '93',
    #  'sensor_gp_hum': '57.63',
    #  'sensor_gp_pres': '101884',
    #  'sensor_gp_tc': '26.280',
    #  'sensor_str': 'Node is OK',
    #  'sequence_num': '0',
    #  'serial_id': '397C530E695B4A72',
    #  'snr': 8.0,
    #  'timestamptz': datetime.datetime(2016, 7, 20, 11, 53, 23, 544000),
    #  'timestring': u'2016-07-20T11:53:23.544000Z',
    #  'waspmote_id': 'NTNU_CTT_22'}

    # MQTT dictionary:
    # {u'data': u'PD0+gAIjNDAzNDcyMzk4I0NUVF9Nb2R1bGUxIzIwOSNHUF9DTzI6MjU0LjU2OSNCQVQ6OTYj',
    #  u'dataRate': u'SF7BW125',
    #  u'frequency': 868.1,
    #  u'gatewayEui': u'AA555A0008060252',
    #  u'nodeEui': u'02032201',
    #  u'rawData': u'QAEiAw7UrIJJuVXgnmfW7zGNNyfzZH7Rhbs9noM2J42iFBAiz/wA7nRZX2CMKsqekHnA==',
    #  u'rssi': -89,
    #  'sensors': {'SENSOR_BAT': '96', 'SENSOR_GP_CO2': '254.569'},
    #  'sequence_Num': '209',
    #  'serial_id': '403472398',
    #  u'snr': 9.2,
    #  u'time': u'2016-08-02T12:43:36.981237138Z',
    #  'waspmote_id': 'CTT_Module1'}

 

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("Rx msg: '")
    pprint(str(msg.payload))
    pprint(msg.payload)
    msgMQTT = json.loads(msg.payload)
    base64EncodedPayload = CTT_Nodes.decode_msg_payload(msgMQTT['data'])
    payload = CTT_Nodes.extract_payload(base64EncodedPayload)
    msgMQTT.update(payload)
    msgDict = map_msg_MQTT_to_monetdb(msgMQTT)
    if db != None:
        mdb.add_node_message(db=db, msg=msgDict)
        db.commit()        

# Per topic message callbacks
def on_message_packets(client, userdata, msg):
    print("Youuuhhouuuu... one more CTT message!!!!\n")
    on_message(client, userdata, msg)
    


def ctt_collect_MQTT_msg():
    node_IDs = ["02032220",
                "02032221",
                "02032222",
                "02032201"]
    tpcs = ["nodes/"+nID+"/packets" for nID in node_IDs]
    set_topics(tpcs)
    
    ### Create a client instance
    ### (parameters descp. here: http://anaconda.org/pypi/paho-mqtt#installation)
    clientMQTT = paho.Client(client_id="Patechoc",
                             clean_session=False,
                             userdata=None,
                             protocol=paho.MQTTv311)
    username = "patrick.merlot@gmail.com"
    password = None
    clientMQTT.username_pw_set(username, password)

    ### Connect to a broker using one of the connect*() functions
    clientMQTT.on_connect = on_connect
    clientMQTT.on_message = on_message
    clientMQTT.connect(host="croft.thethings.girovito.nl", port=1883, keepalive=60, bind_address="")

    ### Call one of the loop*() functions to maintain network traffic flow with the broker
    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    clientMQTT.loop_forever()

    ### Use subscribe() to subscribe to a topic and receive messages
    ### Use publish() to publish messages to the broker
    ### Use disconnect() to disconnect from the broker



def main():
    ctt_collect_MQTT_msg()



if __name__ == "__main__": 
    main()

