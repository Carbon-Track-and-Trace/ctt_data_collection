#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    A collection of function to deal with the nodes, and the data they generates
    ~~~~~~~~
    :copyright: (c) 2016 by Fredrik Anthonisen & Patrick Merlot.
"""

import os, sys, re
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
    labelGasSensors = ["SENSOR_GP_CO2",
                       "SENSOR_GP_NO2",
                       "SENSOR_GP_TC",
                       "SENSOR_GP_HUM",
                       "SENSOR_GP_PRES"]
    labelPartclSensors = ["SENSOR_OPC_PM1",
                          "SENSOR_OPC_PM2_5",
                          "SENSOR_OPC_PM10"]
    labelBatteryLevel = ["SENSOR_BAT"]
    inHEX = decode_base64_to_base16(base64_payload_MQTT)
    temp = inHEX[36:]
    measurements = []
    labelSensors = []
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
            measurements[x] = litte_to_big_endian(measurements[x])
            measurements[x] = struct.unpack('!f', measurements[x].decode('hex'))[0]
        measurements[5] = battery_conversion("".join(measurements[5]))
    dataDict = dict(zip(labelSensors, measurements))
    return dataDict

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
    print BASE64_str
    HEX_str = decode_base64_to_base16(BASE64_str)
    print "payload in HEX:"
    print HEX_str
    payload_dict = extract_payload(BASE64_str)    
    print payload_dict


if __name__ == "__main__": 
    BASE64_str = "PD0+ADdk4lcYVkpDVFQwMSNgj0hmtUOJAAAAAJD2KJpBkoDlkUKTsATGR5fJMoU/mKRU8j+Z4BE5QDRa"
    test_data_extraction(BASE64_str)
