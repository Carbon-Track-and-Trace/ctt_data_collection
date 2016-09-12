#!/usr/bin/env python

import unittest
import collections
import CTT_TTN_MQTT_API as MQTT
import CTT_monetdb_API as mdb
import CTT_Nodes

class store_MQTT_messages(unittest.TestCase):
    def setUp(self):
        self.MQTT_dict = {"payload":"PD0+ACjlGVgYVEtDVFQwOSMIjxtatkOJAAAAAJAAAHBBkgAqdkKTM3fERzRk",
                          "port":3,
                          "counter":25,
                          "dev_eui":"00000000CD3BE279",
                          "metadata":[{"frequency":868.5,
                                       "datarate":"SF7BW125",
                                       "codingrate":"4/5",
                                       "gateway_timestamp":1437236731,
                                       "channel":2,
                                       "server_time":"2016-09-11T15:44:12.805820483Z",
                                       "rssi":-109,
                                       "lsnr":2,
                                       "rfchain":1,
                                       "crc":1,
                                       "modulation":"LORA",
                                       "gateway_eui":"AA555A0008060252",
                                       "altitude":21,
                                       "longitude":10.39562,
                                       "latitude":63.42243},
                                      {"frequency":868.5,
                                       "datarate":"SF7BW125",
                                       "codingrate":"4/5",
                                       "gateway_timestamp":1456741779,
                                       "channel":2,
                                       "server_time":"2016-09-11T15:44:12.807531828Z",
                                       "rssi":-114,
                                       "lsnr":-6.2,
                                       "rfchain":1,
                                       "crc":1,
                                       "modulation":"LORA",
                                       "gateway_eui":"008000000000BC6C",
                                       "altitude":21,
                                       "longitude":10.3857,
                                       "latitude":63.42883}]}


    def test_store_MQTT_msg_to_monetDB(self):
        msgMQTT = self.MQTT_dict
        base64EncodedPayload = msgMQTT['payload']
        meta = msgMQTT.pop('metadata')
        payload  = CTT_Nodes.extract_payload(base64EncodedPayload)
        metadata = CTT_Nodes.extract_info_metadata(meta[0])
        msgMQTT['node_eui'] = msgMQTT['dev_eui']
        msgMQTT.update(payload)
        msgMQTT.update(metadata)
        del msgMQTT["gateway_timestamp"]
        ### msgDict = map_msg_MQTT_to_monetdb(msgMQTT)
        print "MQTT message for human"
        print msgMQTT
        db = mdb.open_connection(database="ctt", port=54321, hostname="localhost",
                                 username="co2", password="ctt",
                                 autocommit=False)
        print "DB status (if None: restart MonetDB on server): ", db
        if db != None:
            mdb.add_node_message(db=db, msg=msgMQTT)
            db.commit()        
            return True
        else:
            print "DB status is None: restart MonetDB on server"
            return False



if __name__ == '__main__':
    unittest.main()
