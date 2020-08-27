import pandas as pd
import json
import warnings

FILE_NAME = "result_ekat.csv"


class Geo:
    """Class for pretty print response"""

    def __init__(self, region, district, sub_district, city,
                 street, house_number, lon, lat):
        self.region = region
        self.district = district
        self.sub_district = sub_district
        self.city = city
        self.street = street
        self.house_number = house_number
        self.location = str(lat) + ", " + str(lon)

    def to_json(self):
        """convert dict with geo to json"""
        geo_dict = {
            "Координаты": self.location,
            "Страна": "Россия",
            "Регион": self.region,
            "Населенный пункт": self.city,
            "Улица": self.street,
            "Дом": int(self.house_number)
        }
        return json.dumps(geo_dict, indent=4, ensure_ascii=False)


class NodesFinder:
    """Class for find request in db"""

    def __init__(self, request):
        self.request = request
        self.request_db = pd.DataFrame()
        self.mean_lon_lat_db = pd.DataFrame()
        self.request_list = self.parse_request(request)
        self.db = self.read_csv()

    @staticmethod
    def parse_request(request):
        """parsing request"""
        return request.split(", ")

    @staticmethod
    def read_csv():
        """read csv file"""
        data = pd.read_csv(FILE_NAME)
        return data

    def get_id_contains_request(self):
        """Find part of request in column of db
        and unit it"""
        cities = self.get_data_frame_with_place(self.db.city,
                                                self.db,
                                                self.request_list)
        if cities.empty is False:
            streets = self.get_data_frame_with_place(self.db.street,
                                                     cities,
                                                     self.request_list)
            if streets.empty is False:
                house_numbers = self.get_data_frame_with_place(
                    self.db.housenumber,
                    streets, self.request_list)
                if house_numbers.empty is False:
                    self.get_mean_lon_lat(house_numbers)
                    return

        streets = self.get_data_frame_with_place(self.db.street,
                                                 self.db,
                                                 self.request_list)
        if streets.empty is False:
            house_numbers = self.get_data_frame_with_place(
                    self.db.housenumber,
                    streets, self.request_list)
            if house_numbers.empty is False:
                self.get_mean_lon_lat(house_numbers)
                return

    @staticmethod
    def get_data_frame_with_place(series, db, request):
        """find part of request in column"""
        place_df = pd.DataFrame()
        warnings.filterwarnings("ignore")
        for place in request:
            buf = place
            try:
                place = int(buf)
            except:
                pass
            find_df = db[place == series]
            if find_df.empty is False:
                place_df = find_df
        return place_df

    def get_mean_lon_lat(self, db):
        """get middle of all lon and all lat for request"""
        self.mean_lon_lat_db = db.groupby('way_ref')[['lon', 'lat']].mean()
        self.request_db = db

    def create_geo(self):
        """do geo object from request db"""
        try:
            if self.request_db.city.notna().sum() > 0:
                city = self.request_db.city.unique()[0]
            else:
                city = "Екатеринбург"
        except Exception:
            pass
        if self.request_db.street.notna().sum() > 0:
            street = self.request_db.street.unique()[0]
        if self.request_db.housenumber.notna().sum() > 0:
            house_number = self.request_db.housenumber.unique()[0]
        lon = self.mean_lon_lat_db.lon.iloc[(self.mean_lon_lat_db.shape[0]-1)]
        lat = self.mean_lon_lat_db.lat.iloc[self.mean_lon_lat_db.shape[0]-1]
        geo = Geo("Свердловская область", "", "",
                  city, street, house_number, lon, lat)
        return geo.to_json()


