import re
import pandas as pd
from os import path

FILE = path.join('data/ekatpart.osm')


class OSM_handler:
    """Class for parsing osm file"""

    def __init__(self):
        self.df_relation = pd.DataFrame(columns=['node_id',
                                                 'way_ref',
                                                 'region',
                                                 'district',
                                                 'subdistrict',
                                                 'city',
                                                 'street',
                                                 'housenumber',
                                                 'housename',
                                                 'suburb',
                                                 'province',
                                                 'place'])
        self.df_way = pd.DataFrame(columns=['way_ref', 'node_id'])
        self.df_node = pd.DataFrame(columns=['node_id', 'lon', 'lat'])

    def get_relations(self):
        """Find in osm file all relations tags"""

        pattern_relation_start = re.compile(r'.*<relation id="\d*".*">')
        pattern_relation_stop = re.compile(r'.*</relation>')
        pattern = ""
        is_relation = False
        with open(FILE, encoding='utf-8', buffering=1) as f:
            for row in f:
                if pattern_relation_start.match(row):
                    pattern += row
                    is_relation = True
                elif is_relation and \
                        pattern_relation_stop.match(row) is None:
                    pattern += row
                elif pattern_relation_stop.match(row):
                    self.handle_relation(pattern)
                    is_relation = False
                    pattern = ""

    def handle_relation(self, relation):
        """Parse included tags in relations
        and add data in relations DataFrame"""

        relation = relation.split("\n")
        pattern_tag = re.compile(
            r'.*<tag k="addr:(.*)" v="(.*)"/>')
        pattern_member = re.compile(
            r'.*<member type="(.*)" ref="(\d*)".*/>')
        region = ""
        suburb = ""
        province = ""
        place = ""
        district = ""
        sub_district = ""
        city = ""
        street = ""
        house_number = ""
        house_name = ""
        node_id = 0
        way_list = []
        for row in relation:
            pattern = pattern_tag.match(row)
            if pattern:
                key = pattern.group(1)
                value = pattern.group(2)
                if key == 'region':
                    region = value
                elif key == 'district':
                    district = value
                elif key == 'subdistrict':
                    sub_district = value
                elif key == 'city':
                    city = value
                elif key == 'street':
                    street = value
                elif key == 'housenumber':
                    house_number = value
                elif key == 'housename':
                    house_name = value
                elif key == 'place':
                    place = value
                elif key == 'province':
                    province = value
                elif key == 'suburb':
                    suburb = value
        if region or district or sub_district \
                or city or street or house_number \
                or house_name or suburb or place \
                or province:
            for row in relation:
                pattern = pattern_member.match(row)
                if pattern:
                    type_member = pattern.group(1)
                    ref = pattern.group(2)
                    if type_member == "node":
                        node_id = ref
                        break
                    elif type_member == "way":
                        way_list.append(ref)
        if node_id:
            self.df_relation = self.df_relation.append({
                'node_id': node_id,
                'way_ref': None,
                'region': region,
                'district': district,
                'subdistrict': sub_district,
                'city': city,
                'street': street,
                'housenumber': house_number,
                'housename': house_name,
                'place': place,
                'province': province,
                'suburb': suburb},
                ignore_index=True)
        else:
            for ref in way_list:
                self.df_relation = self.df_relation.append({
                    'node_id': None,
                    'way_ref': ref,
                    'region': region,
                    'district': district,
                    'subdistrict': sub_district,
                    'city': city,
                    'street': street,
                    'housenumber': house_number,
                    'housename': house_name,
                    'place': place,
                    'province': province,
                    'suburb': suburb},
                    ignore_index=True)

    def get_ways(self):
        """Find in osm file all way tags"""

        pattern_way_start = re.compile(r'.*<way id="(\d*)".*">')
        pattern_way_stop = re.compile(r'.*</way>')
        pattern = ""
        is_way = False
        id = ""
        way_ref = self.df_relation.way_ref.unique().tolist()
        with open(FILE, encoding='utf-8', buffering=1) as f:
            for row in f:
                pattern_way = pattern_way_start.match(row)
                if pattern_way:
                    id = pattern_way.group(1)
                    if id in way_ref:
                        pattern += row
                        is_way = True
                elif is_way and id and \
                        pattern_way_stop.match(row) is None:
                    pattern += row
                elif is_way and id and \
                        pattern_way_stop.match(row):
                    self.handle_way(pattern, id)
                    is_way = False
                    pattern = ""
                    id = ""

    def handle_way(self, way, id_way):
        """Parse included tags in way
            and add data in relations DataFrame"""

        way = way.split("\n")
        pattern_nd = re.compile(r'.*<nd ref="(\d*)"/>')
        for row in way:
            pattern = pattern_nd.match(row)
            if pattern:
                ref = pattern.group(1)
                self.df_way = self.df_way.append({'node_id': ref,
                                                  'way_ref': id_way},
                                                 ignore_index=True)

    def get_nodes(self):
        """Find in osm file all node tags"""

        # pattern_node = re.compile(
        #     r'.*<node id="(\d*)" lat="(\d*.\d*)" lon="(\d*.\d*)".*>')
        pattern_node_second = re.compile(
            r'.*<node id="(\d*)".*lat="(\d*.\d*)" lon="(\d*.\d*)".*>')
        id_list = self.df_way.node_id.unique().tolist()
        with open(FILE, encoding='utf-8', buffering=1) as f:
            for row in f:
                pattern = pattern_node_second.match(row)
                if pattern:
                    id_node = pattern.group(1)
                    if id_node in id_list:
                        lat = pattern.group(2)
                        lon = pattern.group(3)
                        self.handle_node(id_node, lat, lon)

    def handle_node(self, id_node, lat, lon):
        """Parse included tags in node
            and add data in relations DataFrame"""

        self.df_node = self.df_node.append({'node_id': id_node,
                                            'lon': lon,
                                            'lat': lat},
                                           ignore_index=True)

    def merge_all_data(self):
        """Aggregate data frames: node, relation, way"""

        df_common = self.df_way.merge(self.df_node,
                                      on='node_id', how="inner")
        df_common = df_common[df_common.lon.notna()]
        df_common = df_common.merge(self.df_relation,
                                    on='way_ref')
        df_common = df_common.drop_duplicates()
        df_common.to_csv("result_ekat.csv")

    def main(self):
        """Do all the data preparation work"""

        self.get_relations()
        self.get_ways()
        self.get_nodes()
        self.merge_all_data()
