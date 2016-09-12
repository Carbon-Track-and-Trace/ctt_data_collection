#!/usr/bin/env python

import unittest
import collections
import CTT_Nodes as Nodes


class read_node_messages(unittest.TestCase):
    def setUp(self):
        self.BASE64_str = "PD0+ADdk4lcYVkpDVFQwMSNgj0hmtUOJAAAAAJD2KJpBkoDlkUKTsATGR5fJMoU/mKRU8j+Z4BE5QDRa"
        self.HEX_str = "3c3d3e003764e25718564a435454303123608f4866b543890000000090f6289a419280e5914293b004c64797c932853f98a454f23f99e0113940345a"
        self.payload = {'SENSOR_GP_TC': 19.270000457763672,
                        'SENSOR_OPC_PM1': 1.0406123399734497,
                        'SENSOR_BAT': 90,
                        'SENSOR_GP_NO2': 0.0,
                        'SENSOR_OPC_PM2_5': 1.8932080268859863,
                        'SENSOR_GP_CO2': 362.799072265625,
                        'SENSOR_GP_HUM': 72.9482421875,
                        'SENSOR_OPC_PM10': 2.8917160034179688,
                        'SENSOR_GP_PRES': 101385.375}

    def test_convert_base64_to_HEX(self):
        print "\nbase64 input:\n", self.BASE64_str
        HEX_str = Nodes.decode_base64_to_base16(self.BASE64_str)
        print "HEX output:\n", HEX_str
        self.assertEqual(HEX_str.strip(), self.HEX_str.strip())

    def test_extract_payload(self):
        print "\nbase64 input:\n", self.BASE64_str
        d1 = Nodes.extract_payload(self.BASE64_str)    
        print "\nPayload dict. in output:\n", d1
        d2 = self.payload
        for k,v1 in d1.iteritems():
            self.assertIn(k, d2)
            v2 = d2[k]
            if(isinstance(v1, collections.Iterable) and
               not isinstance(v1, basestring)):
                self.assertItemsEqual(v1, v2)
            else:
                self.assertEqual(v1, v2)
        return True

if __name__ == '__main__':
    unittest.main()
