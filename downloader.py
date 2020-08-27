from urllib.request import urlretrieve
import os.path


def load_data():
    """Download osm file and save it"""

    # скачивается часть Екатеринбурга
    link = "https://api.openstreetmap.org/api/0.6/map?bbox=60.5878,56.8299,60.6309,56.8499"
    urlretrieve(link, os.path.join('data', 'ekatpart.osm'))
