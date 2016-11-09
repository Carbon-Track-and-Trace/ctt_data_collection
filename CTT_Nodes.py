#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    A collection of function to deal with the nodes, and the data they generates
    ~~~~~~~~
    :copyright: (c) 2016 by Fredrik Anthonisen & Patrick Merlot.
"""

import os, sys, re
import argparse
from sys import stdin, stderr
import json
from pprint import pprint
from datetime import datetime
import base64 
from collections import deque
import struct

def litte_to_big_endian(hex_number):
    temp = ""
    for x in range(0, len(hex_number)):
        if(x%2 != 0):
            temp = hex_number[x-1: x+1] + temp
    return temp


def battery_conversion(valueInHEX):
    hex_letter = str(valueInHEX).upper()
    try:
        return int(hex_letter, 16)
    except ValueError:
        return hex_letter

def extract_payload(base64_payload_MQTT):
    """ From a device's payload in base64 like this one:
    PD0+ADdk4lcYVkpDVFQwMSNgj0hmtUOJAAAAAJD2KJpBkoDlkUKTsATGR5fJMoU/mKRU8j+Z4BE5QDRa
    
    this function converts it in HEX, then parse it according
    to this arbitrary order:
    CO2 = "SENSOR_GP_CO2",
    NO2 = "SENSOR_GP_NO2",
    Temperature = "SENSOR_GP_TC",
    Humidity = "SENSOR_GP_HUM",
    Pressure = "SENSOR_GP_PRES".
    (optional) PM1 = "SENSOR_OPC_PM1",
    (optional) PM2.5 = "SENSOR_OPC_PM2_5",
    (optional) PM10 = "SENSOR_OPC_PM10",
    Battery BAT  = "SENSOR_BAT"
    it returns the data as dictionary
    """
    inHEX = decode_base64_to_base16(base64_payload_MQTT)
    (dataDict, measurements) = extract_payload_fromHEX(inHEX)
    return dataDict

def extract_payload_fromHEX(inHEX_payload_MQTT):
    """ From a device's payload in HEXADECIMAL (base16) like this one:
    PD0+ADdk4lcYVkpDVFQwMSNgj0hmtUOJAAAAAJD2KJpBkoDlkUKTsATGR5fJMoU/mKRU8j+Z4BE5QDRa
    
    this function converts it in HEX, then parse it according
    to this arbitrary order:
    CO2 = "SENSOR_GP_CO2",
    NO2 = "SENSOR_GP_NO2",
    Temperature = "SENSOR_GP_TC",
    Humidity = "SENSOR_GP_HUM",
    Pressure = "SENSOR_GP_PRES".
    (optional) PM1 = "SENSOR_OPC_PM1",
    (optional) PM2.5 = "SENSOR_OPC_PM2_5",
    (optional) PM10 = "SENSOR_OPC_PM10",
    Battery BAT  = "SENSOR_BAT"
    it returns the data as dictionary
    """
    labelGasSensors = ["SENSOR_GP_CO2",
                       "SENSOR_GP_NO2",
                       "SENSOR_GP_TC",
                       "SENSOR_GP_HUM",
                       "SENSOR_GP_PRES"]
    labelPartclSensors = ["SENSOR_OPC_PM1",
                          "SENSOR_OPC_PM2_5",
                          "SENSOR_OPC_PM10"]
    labelBatteryLevel = ["SENSOR_BAT"]
    temp = inHEX_payload_MQTT[36:]
    measurements = []
    labelSensors = []

    # "Just battery, no measurement, possible if level battery < 40%"
    if len(temp)<5:
        measurements.append(temp[2:4]) # %Battery
        measurements[0] = battery_conversion("".join(measurements[0]))
        labelSensors.extend(labelBatteryLevel)
        dataDict = dict(zip(labelSensors, measurements))
        return (dataDict, measurements)
    measurements.append(temp[2:10])  # CO2
    measurements.append(temp[12:20]) # NO2
    measurements.append(temp[22:30]) # Temperature
    measurements.append(temp[32:40]) # Humidity
    measurements.append(temp[42:50]) # Pressure
    labelSensors = labelGasSensors
    # "more sensors"
    if(len(temp) > 55):
        labelSensors.extend(labelPartclSensors)
        measurements.append(temp[52:60]) # PM1
        measurements.append(temp[62:70]) # PM2.5
        measurements.append(temp[72:80]) # PM10
        measurements.append(temp[82:84]) # %Battery
    # "less sensors"
    else:
        measurements.append(temp[52:54]) # %Battery
    labelSensors.extend(labelBatteryLevel)
    if len(measurements) == 9:
        for x in range(0, 8):
            measurements[x] = litte_to_big_endian(measurements[x])
            measurements[x] = struct.unpack('!f', measurements[x].decode('hex'))[0]
        measurements[8] = battery_conversion("".join(measurements[8]))
        
    else:
        for x in range(0, 5):
            print x
            measurements[x] = litte_to_big_endian(measurements[x])
            measurements[x] = struct.unpack('!f', measurements[x].decode('hex'))[0]
        measurements[5] = battery_conversion("".join(measurements[5]))
    dataDict = dict(zip(labelSensors, measurements))
    return (dataDict, measurements)

def extract_info_metadata(metadataDict):
    """ From a device's metadata like this one:
           u'metadata': [{u'altitude': -3,
                           u'channel': 5,
                           u'codingrate': u'4/5',
                           u'crc': 1,
                           u'datarate': u'SF7BW125',
                           u'frequency': 867.5,
                           u'gateway_eui': u'0000024B080E06B3',
                           u'gateway_time': u'2016-08-28T21:09:38.193622Z',
                           u'gateway_timestamp': 2458809372,
                           u'latitude': 55.7079,
                           u'longitude': 9.53253,
                           u'lsnr': 7,
                           u'modulation': u'LORA',
                           u'rfchain': 0,
                           u'rssi': -65,
                           u'server_time': u'2016-08-28T21:09:38.208945555Z'}],
    """
    return metadataDict



def extract_payload_Trondheim_v01(payload):
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

def decode_base16_to_base64(HEX_str):
    """Decode a base16 (Hexadecimal) message to base64"""
    decoded = HEX_str.decode("hex").encode("base64")
    return decoded

def decode_base64_to_base16(BASE64_str):
    """Decode a base64 message into Hexadecimal (base16)  """
    decoded = BASE64_str.decode("base64").encode("hex")
    return decoded
        
def decode_msg_payload(base64EncodedPayload):
    payload = base64.b64decode(base64EncodedPayload)
    return payload

def test_data_extraction(BASE64_str):
    print "payload in base64:"
    pprint(BASE64_str)
    HEX_str = decode_base64_to_base16(BASE64_str)
    print "payload in HEX:"
    pprint(HEX_str)
    payload_dict = extract_payload(BASE64_str)    
    print "Human-readable payload:"
    pprint(payload_dict)

def extract_data_from_file(pathToFile, encoding="base16"):
    objs = []
    with open(pathToFile) as f:
        msgs = f.readlines()
    if encoding == "base16":
        for msg in msgs:
            (dataDict, measurements) = extract_payload_fromHEX(msg)
            objs.append(dataDict)
    elif encoding == "base64":
            payload_dict = extract_payload(args.base64)
            objs.append(payload_dict)
    else:
        print "You must specify one of the possible encoding system:"
        print "encoding='base64' or encoding='base16'"
        sys.exit()
    return objs

def test_data_extraction(BASE64_str):
    print "payload in base64:"
    pprint(BASE64_str)
    HEX_str = decode_base64_to_base16(BASE64_str)
    print "payload in HEX:"
    pprint(HEX_str)
    payload_dict = extract_payload(BASE64_str)    
    print "Human-readable payload:"
    pprint(payload_dict)



if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-hex", "--base16", type=str,
                       help="Read and Extract payload from a message in hexadecimal,\
                       such as 3c3d3e003764e25718564a435454303123608f4866b543890000000090f6289a419280e5914293b004c64797c932853f98a454f23f99e0113940345a")
    group.add_argument("-b64", "--base64", type=str,
                       help="Read and Extract payload from a message in base64,\
                       such as PD0+ADdk4lcYVkpDVFQwMSNgj0hmtUOJAAAAAJD2KJpBkoDlkUKTsATGR5fJMoU/mKRU8j+Z4BE5QDRa")
    parser.add_argument("-f", "--file_path", type=str, help="Input file to read data from to be store in DB, formatted as dictionaries on each line.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    args = parser.parse_args()
    if args.file_path:
        if args.base16:
            objs = extract_data_from_file(pathToFile, encoding="base16")
        elif args.base64:
            objs = extract_data_from_file(pathToFile, encoding="base64")
        else:
            objs = extract_data_from_file(pathToFile, encoding="unknown")
        pprint(objs)
    else:
        if args.base16:
            (dataDict, measurements) = extract_payload_fromHEX(args.base16)
            if args.verbose:
                print "Human-readable payload:"
                pprint(dataDict)
                print "List of measurements as ordered within the device:"
                pprint(measurements)
            else:
                pprint(dataDict)
        elif args.base64:
            if args.verbose:
                payload_dict = test_data_extraction(args.base64)
            else:
                payload_dict = extract_payload(args.base64)
                pprint(payload_dict)            
        else:
            print "Check input commands with:\npython CTT_Nodes.py -h"
