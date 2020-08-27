import unittest
from geo_node import NodesFinder
from downloader import *
from data_handler import *
from os import path


class Test(unittest.TestCase):

    load_data()
    osm_handler = OSM_handler()
    osm_handler.main()
    request = "Екатеринбург, улица Шевченко, 9"
    geo_nodes = NodesFinder(request)
    geo_nodes.get_id_contains_request()
    geo = geo_nodes.create_geo()

    def test_load(self):
        path_ = path.join("data/ekatpart.osm")
        self.assertEqual(path.exists(path_), True)

    def test_handler(self):
        self.assertEqual(path.exists("result_ekat.csv"), True)

    def test_different_order(self):
        request_second = "улица Шевченко, 9"
        result = NodesFinder(request_second)
        result.get_id_contains_request()
        self.assertEqual(result.create_geo(), self.geo)

    def test_different_order_second(self):
        request_second = "9, Екатеринбург, улица Шевченко"
        result = NodesFinder(request_second)
        result.get_id_contains_request()
        self.assertEqual(result.create_geo(), self.geo)


if __name__ == '__main__':
    unittest.main()
