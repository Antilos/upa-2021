import os, json
from io import StringIO
import requests
import pandas as pd
import pymongo

class UnexpectedDataFormatException(Exception):
    def __init__(self, msg):
        super.__init__(f"Unexpected data format: {msg}")

class DataDownloader:
    def __init__(self, nrpzs_data_url="https://nrpzs.uzis.cz/res/file/export/export-sluzby-2021-10.csv", czso_data_url = "https://www.czso.cz/documents/62353418/143522504/130142-21data043021.csv/760fab9c-d079-4d3a-afed-59cbb639e37d?version=1.1", folder="data"):
        self.nrpzs_data_url = nrpzs_data_url
        self.czso_data_url = czso_data_url
        self.folder = folder

    def download_data(self):
        #create the target directory if it doesn't exist
        if not os.path.isdir(self.folder):
            os.mkdir(self.folder)

        with requests.Session() as s:
            #set headers
            s.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'})

            #get data from nrpzs
            r = s.get(self.nrpzs_data_url)
            r.raise_for_status()
            decoded_content = r.content.decode('latin1')
            nrpzs_df = pd.read_csv(StringIO(decoded_content), sep=';', quotechar='"')

            #get data from czso
            r = s.get(self.czso_data_url)
            r.raise_for_status()
            decoded_content = r.content.decode('latin1')
            czso_df = pd.read_csv(StringIO(decoded_content), sep=',', quotechar='"')
        
        return nrpzs_df, czso_df

def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb+srv://xkocal00:xkocal00_upa_2021@xkocal00-upa-2021.0z2ty.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = pymongo.MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['upa']

def mongoimport(csv_path, db_name, coll_name, db_url='localhost', db_port=27000):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    client = pymongo.MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv(csv_path)
    payload = json.loads(data.to_json(orient='records'))
    coll.remove()
    coll.insert(payload)
    return coll.count()

def import_df_to_mongo(df, db, coll_name):
    coll = db[coll_name]
    payload = json.loads(df.to_json(orient='records'))
    coll.remove()
    coll.insert(payload)
    return coll.count()

if __name__ == '__main__':
    downlader = DataDownloader()
    data = downlader.download_data()

    db = get_database()
    n = import_df_to_mongo(data[0], db, "nrpzs")
    print(f"Imported {n} documents to collection nrpzs")
    n = import_df_to_mongo(data[1], db, "czso")
    print(f"Imported {n} documents to collection czso")