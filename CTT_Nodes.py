#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    A collection of functions to deal with the nodes and their data frames
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


def extract_payload(base64_payload_MQTT):
    dataDict = extract_payload_fromBase64(base64_payload_MQTT)
    return dataDict

    
def extract_payload_fromBase64(base64_payload_MQTT):
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
    dataDict = extract_BINARY_FRAME_fromHEX(inHEX)
    return dataDict

def extract_BINARY_FRAME_fromHEX(inHEX_MQTT):
    dataDict = {}
    #print inHEX_MQTT
    #print inHEX_MQTT.decode('hex')

    ### Assuming the data frame follows version 12 from Libellium standard
    ### http://www.libelium.com/v12/development/waspmote/documentation/data-frame-guide-v12/
    
    # BINARY HEADER
    elements = [('startDelimiter', 3),
                ('frameType', 1),
                ('totalNbBytes', 1),
                ('serialID', 4),
                ('waspmoteID', -1), ## VJCTT01, TKCTT05, ... 7 characters but can't rely on arbitrary choices
                ('separator', 1),
                ('frameSequence', 1)]
    #pprint(elements)
    header = {}
    for k,length in elements:
        header[k]={}
        header[k]['length'] = length
    startByte = 0
    for k,l in elements:
        #print "\n",k
        ## treat as STRING
        if k in ['startDelimiter', 'waspmoteID', 'separator']:
            if k == 'waspmoteID': ## special case, since un-predifined length to be determined
                rest = inHEX_MQTT[startByte:]
                wspmtID = rest.split("23")[0]
                header['waspmoteID']['length'] = len(wspmtID)/2
            header[k]['valueHEX'] = inHEX_MQTT[startByte:startByte+2*header[k]['length']]
            header[k]['value'] = header[k]['valueHEX'].decode('hex')
            startByte += 2 * header[k]['length']
            #print "header[{k}]['length']: {l}".format(k=k,l=header[k]['length'])
            #pprint(header[k]) 
        ## treat as INT
        else:
            # remember 1 Byte coded by 2 HEX digits
            header[k]['valueHEX'] = inHEX_MQTT[startByte:startByte+2*header[k]['length']]
            #print struct.unpack('h', '8f'.decode('hex'))
            header[k]['valueDEC'] = int(header[k]['valueHEX'], 16) # assuming having integers only to convert
            #header[k]['valueDEC'] = int(header[k]['valueHEX'], 16) 
            startByte += 2 * header[k]['length']
            #print "header[{k}]['length']: {l}".format(k=k,l=header[k]['length']) 
            #print "startByte: ",startByte
            #pprint(header[k]) 
    #pprint(header)
    dataDict['header'] = header

    # BINARY PAYLOAD (= list of {sensorID + sensorMeasurement})
    ### sensors_v12 is built from the table of Sensor Fields p.10 of this document:
    ### http://www.libelium.com/v12/development/waspmote/documentation/data-frame-guide-v12/
    sensors_v12=[{'sensorName':"Battery", 'sensorRef': "N/A",'sensorTag': 'SENSOR_BAT', 'sensorID': 52, 'sensorASCII': 'BAT','nbFields':1, 'variableType': 'uint8_t','bytesPerField': 1, 'decimalPrecision': 0, 'unit': '%'},
                 {'sensorName':"Carbon Monoxide", 'sensorRef':"9229", 'sensorTag':'SENSOR_CO','sensorID':0,'sensorASCII':'CO','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Carbon Dioxide", 'sensorRef':"9230", 'sensorTag':'SENSOR_CO2','sensorID':1,'sensorASCII':'CO2','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Oxygen", 'sensorRef':"9231", 'sensorTag':'SENSOR_O2','sensorID':2,'sensorASCII':'O2','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Methane", 'sensorRef':"9232", 'sensorTag':'SENSOR_CH4','sensorID':3,'sensorASCII':'CH4','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Liquefied Petroleum Gases", 'sensorRef':"9234", 'sensorTag':'SENSOR_LPG','sensorID':4,'sensorASCII':'LPG','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Ammonia", 'sensorRef':"9233", 'sensorTag':'SENSOR_NH3','sensorID':5,'sensorASCII':'NH3','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Air Pollutants 1", 'sensorRef':"9235", 'sensorTag':'SENSOR_AP1','sensorID':6,'sensorASCII':'AP1','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Air Pollutants 2", 'sensorRef':"9236", 'sensorTag':'SENSOR_AP2','sensorID':7,'sensorASCII':'AP2','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Solvent Vapors", 'sensorRef':"9237", 'sensorTag':'SENSOR_SV','sensorID':8,'sensorASCII':'SV','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Nitrogen Dioxide", 'sensorRef':"9238", 'sensorTag':'SENSOR_NO2','sensorID':9,'sensorASCII':'NO2','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Ozone", 'sensorRef':"9258", 'sensorTag':'SENSOR_O3','sensorID':1,'sensorASCII':' O3','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Hydrocarbons", 'sensorRef':"9201", 'sensorTag':'SENSOR_VOC','sensorID':1,'sensorASCII':' VOC','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "voltage"},
                 {'sensorName':"Temperature Celsius", 'sensorRef':"9203", 'sensorTag':'SENSOR_TCA','sensorID':1,'sensorASCII':' TCA','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 2,'unit': "º C"},
                 {'sensorName':"Temperature Fahrenheit", 'sensorRef':"9203", 'sensorTag':'SENSOR_TFA','sensorID':1,'sensorASCII':' TFA','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 2,'unit': "º F"},
                 {'sensorName':"Humidity", 'sensorRef':"9204", 'sensorTag':'SENSOR_HUMA','sensorID':1,'sensorASCII':' HUMA','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 1,'unit': "%RH"},
                 {'sensorName':"Pressure atmospheric", 'sensorRef':"9250", 'sensorTag':'SENSOR_PA','sensorID':1,'sensorASCII':' PA','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 2,'unit': "Kilo Pascales"},
                 {'sensorName':"Pressure/Weight", 'sensorRef':"9219", 'sensorTag':'SENSOR_PW','sensorID':1,'sensorASCII':' PW','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "Ohms"},
                 {'sensorName':"Bend", 'sensorRef':"9218", 'sensorTag':'SENSOR_BEND','sensorID':1,'sensorASCII':' BEND','nbFields': 1,'variableType': 'float','bytesPerField': 4,'decimalPrecision': 3,'unit': "Ohms"},
                 {'sensorName':"Chlorine",'sensorRef': "9386-P", 'sensorTag':'SENSOR_GP_CL2','sensorID':128,'sensorASCII':'GP_CL2','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Carbon Monoxide",'sensorRef': "9371-P", 'sensorTag':'SENSOR_GP_CO','sensorID':129,'sensorASCII':'GP_CO','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Ethylene Oxide",'sensorRef': "9385-P", 'sensorTag':'SENSOR_GP_ETO','sensorID':130,'sensorASCII':'GP_ETO','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Hydrogen",'sensorRef': "9380-P", 'sensorTag':'SENSOR_GP_H2','sensorID':131,'sensorASCII':'GP_H2','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Hydrogen Sulphide",'sensorRef': "9381-P", 'sensorTag':'SENSOR_GP_H2S','sensorID':132,'sensorASCII':'GP_H2S','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Hydrogen Chloride",'sensorRef': "9382-P", 'sensorTag':'SENSOR_GP_HCL','sensorID':133,'sensorASCII':'GP_HCL','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Hydrogen Cyanide",'sensorRef': "9383-P", 'sensorTag':'SENSOR_GP_HCN','sensorID':134,'sensorASCII':'GP_HCN','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Ammonia",'sensorRef': "9378-P", 'sensorTag':'SENSOR_GP_NH3','sensorID':135,'sensorASCII':'GP_NH3','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Nitrogen Monoxide",'sensorRef': "9375-P", 'sensorTag':'SENSOR_GP_NO','sensorID':136,'sensorASCII':'GP_NO','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Nitrogen Dioxide",'sensorRef': "9376-P", 'sensorTag':'SENSOR_GP_NO2','sensorID':137,'sensorASCII':'GP_NO2','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Oxygen",'sensorRef': "9373-P", 'sensorTag':'SENSOR_GP_O2','sensorID':138,'sensorASCII':'GP_O2','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Phospine",'sensorRef': "9384-P", 'sensorTag':'SENSOR_GP_PH3','sensorID':139,'sensorASCII':'GP_PH3','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Sulfur Dioxide",'sensorRef': "9377-P", 'sensorTag':'SENSOR_GP_SO2','sensorID':140,'sensorASCII':'GP_SO2','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Methane",'sensorRef': "9379-P", 'sensorTag':'SENSOR_GP_CH4','sensorID':141,'sensorASCII':'GP_CH4','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"%/LEL"},
                 {'sensorName':"Ozone",'sensorRef': "9374-P", 'sensorTag':'SENSOR_GP_O3','sensorID':142,'sensorASCII':'GP_O3','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Carbon Dioxide",'sensorRef': "9372-P", 'sensorTag':'SENSOR_GP_CO2','sensorID':143,'sensorASCII':'GP_CO2','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ppm"},
                 {'sensorName':"Temperature Celsius",'sensorRef': "9370-P", 'sensorTag':'SENSOR_GP_TC','sensorID':144,'sensorASCII':'GP_TC','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':3,'unit':"ºC"},
                 {'sensorName':"Temperature Fahrenheit",'sensorRef': "9370-P", 'sensorTag':'SENSOR_GP_TF','sensorID':145,'sensorASCII':'GP_TF','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':2,'unit':"ºF"},
                 {'sensorName':"Humidity",'sensorRef': "9370-P", 'sensorTag':'SENSOR_GP_HUM','sensorID':146,'sensorASCII':'GP_HUM','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':2,'unit':"%RH"},
                 {'sensorName':"Pressure",'sensorRef': "9370-P", 'sensorTag':'SENSOR_GP_PRES','sensorID':147,'sensorASCII':'GP_PRES','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':0,'unit':"Pa"},
                 {'sensorName':"Temperature Celsius",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_TC','sensorID':148,'sensorASCII':'TC','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':2,'unit':"ºC"},
                 {'sensorName':"Temperature Fahrenheit",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_TF','sensorID':149,'sensorASCII':'TF','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':2,'unit':"ºF"},
                 {'sensorName':"Pressure",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_P','sensorID':150,'sensorASCII':'P','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':0,'unit':"Pa"},
                 {'sensorName':"PM1",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_PM1','sensorID':151,'sensorASCII':'PM1','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':4,'unit':"μg/m3"},
                 {'sensorName':"PM2.5",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_PM2_5','sensorID':152,'sensorASCII':'PM2_5','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':4,'unit':"μg/m3"},
                 {'sensorName':"PM10",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_PM10','sensorID':153,'sensorASCII':'PM10','nbFields':1,'variableType':'float','bytesPerField':4,'decimalPrecision':4,'unit':"μg/m3"},
                 {'sensorName':"Particle counter",'sensorRef': "9387-P", 'sensorTag':'SENSOR_OPC_PART','sensorID':154,'sensorASCII':'PART','nbFields':2,'variableType':'float','bytesPerField':4,'decimalPrecision':0,'unit':"N/A"}]

    payload = []
    payloadHEX = inHEX_MQTT[startByte:]
    startByte = 0
    while startByte < len(payloadHEX):
        #print ""
        #print 'len(payloadHEX): ',  len(payloadHEX)
        #print "startByte: ", startByte
        #print 'payloadHEX: ',  payloadHEX
        measure = {}
        measure['sensorID_HEX'] = payloadHEX[startByte:startByte+2]
        measure['sensorID'] = int(measure['sensorID_HEX'], 16)
        #pprint(measure)
        startByte += 2
        match = next((s for s in sensors_v12 if s['sensorID'] ==  measure['sensorID']), None)
        #pprint(match)
        measure.update(match)
        length_sensorValue = 2*(measure['nbFields']*measure['bytesPerField'])
        measure['sensorValue_HEX'] = payloadHEX[startByte:startByte + length_sensorValue]

        ## is the sensor value a INT?
        if (measure['variableType'] == 'uint8_t'):
            measure['sensorValue'] = int(measure['sensorValue_HEX'], 16)

            ## any other sensors are probably float
        elif (measure['variableType'] == 'float'):
            ## Little-endian bit ordering use "<" in unpack()
            measure['sensorValue'] = struct.unpack('<f', measure['sensorValue_HEX'].decode('hex'))[0]
        startByte += length_sensorValue
        #print "measure['sensorValue_HEX']: ", measure['sensorValue_HEX']
        #pprint(measure)
        payload.append(measure)
    dataDict['payload'] = payload
    labels = [l['sensorTag'] for l in payload]
    values = [l['sensorValue'] for l in payload]
    output = dict(zip(labels, values))
    #pprint(output)
    return output

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

# def extract_payload_Trondheim_v01(payload):
#     """ From a device's payload like this one:
#     <=> #403472398#CTT_Module1#144#GP_CO2:255.398#BAT:98#
#     this functions returns the following data as dictionary
#     # Serial ID: 403472398
#     # Waspmote ID: CTT_Module1
#     # Sequence: 144
#     # sensor1 key/value: GP_CO2/255.398
#     # sensor2 key/value: BAT/98
#     """
#     ###payload = '<=>\x80\x02#403472398#CTT_Module1#55#GP_CO2:205.790#BAT:96#'    
#     sp = deque(payload.split("#"))
#     prelude = sp.popleft()
#     dataDict = {}
#     dataDict['serial_id']    = sp.popleft()
#     dataDict['waspmote_id']  = sp.popleft()
#     dataDict['sequence_Num'] = sp.popleft()
#     dataDict['sensors'] = {}
#     while len(sp) > 0:
#         sensor = sp.popleft()
#         if sensor.strip() != '': 
#             kv = sensor.split(":")
#             dataDict['sensors']['SENSOR_'+kv[0]] = kv[1]
#     return dataDict

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
            #(dataDict, measurements) = extract_payload_fromHEX(msg)
            dataDict = extract_BINARY_FRAME_fromHEX(msg)
            objs.append(dataDict)
    elif encoding == "base64":
        for msg in msgs:
            dataDict = extract_payload(msg)
            objs.append(dataDict)
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
                       such as 3c3d3e002866155818544b435454303223778ff596d1438900000000903d0a03419200b6b142935a6fc8473428")
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
            dataDict = extract_BINARY_FRAME_fromHEX(args.base16)
            if args.verbose:
                print "Human-readable payload:"
                pprint(dataDict)
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
