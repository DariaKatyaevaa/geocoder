import argparse
from geo_node import NodesFinder
from downloader import *
from data_handler import *


def main():
    if args.r:
        node = NodesFinder(args.r)
        node.get_id_contains_request()
        print(node.create_geo())
    if args.load:
        load_data()
        osm_handler = OSM_handler()
        osm_handler.main()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="Geocoder",
                                     description="This program input text, "
                                                 "such as an address"
                                                 " or the name of a place,"
                                                 " and returning a "
                                                 "latitude/longitude "
                                                 "location in Russia.")
    parser.add_argument("-r", type=str, help="Input geo request for geocoder.")
    parser.add_argument("-load", help="Download osm data",
                        action='store_const', const=True)
    args = parser.parse_args()
    main()
