import os, json, re
import argparse
from io import StringIO
from datetime import datetime
from typing import Optional
import requests
import pandas as pd
import pymongo
from bs4 import BeautifulSoup

class UnexpectedDataFormatException(Exception):
    def __init__(self, msg):
        super.__init__(f"Unexpected data format: {msg}")

class DataDownloader:
    def __init__(
        self,
        nrpzs_url='https://nrpzs.uzis.cz/',
        nrpzs_archive_url="https://nrpzs.uzis.cz/index.php?pg=home--download&archiv=sluzby",
        nrpzs_cols_new = ['ZdravotnickeZarizeniId', 'NazevCely', 'KrajKod', 'OkresKod', 'DatumZahajeniCinnosti', 'OborPece'],
        nrpzs_cols_old = ['ZdravotnickeZarizeniId', 'NazevCely', 'KrajCode', 'OkresCode', 'DatumZahajeniCinnosti', 'OborPece'],
        czso_data_url = "https://www.czso.cz/documents/62353418/143522504/130142-21data043021.csv/760fab9c-d079-4d3a-afed-59cbb639e37d?version=1.1",
        czso_cols = ['hodnota', 'pohlavi_kod', 'vek_kod', 'vuzemi_kod', 'vuzemi_txt'], #vuzemi_cis not needed, we will filter based on it
        folder="data"
        ):
        self.nrpzs_url = nrpzs_url
        self.nrpzs_archive_url = nrpzs_archive_url
        self.nrpzs_data_url_format = "export-sluzby-{}-{}.csv"
        self.nrpzs_cols_new = nrpzs_cols_new
        self.nrpzs_cols_old = nrpzs_cols_old
        self.czso_data_url = czso_data_url
        self.czso_cols = czso_cols
        self.folder = folder

    def download_data(self):
        #create the target directory if it doesn't exist
        if not os.path.isdir(self.folder):
            os.mkdir(self.folder)

        with requests.Session() as s:
            #set headers
            s.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'})
            
            nrpzs_data = []
            #get data from nrpzs
            r = s.get(self.nrpzs_archive_url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            if soup:
                links = soup.find_all("a", href=re.compile(r"export-sluzby-.*?-.*?.csv")) #find all links with data format in href
                for link in links:
                    #download data
                    href = link.get('href')
                    date = re.search(r'export-sluzby-(.*?-.*?).csv', href)[1]
                    print(f"{datetime.now().strftime('%H:%M:%S')}|Downloading data from {date}")
                    with requests.Session() as s2:
                        s2.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'})
                        r2 = s2.get(self.nrpzs_url+link.get('href'))
                        r2.raise_for_status()

                    decoded_content = r2.content.decode('latin2')
                    df_tmp = pd.read_csv(StringIO(decoded_content), dtype=str, sep=';', quotechar='"', header=0, skipinitialspace=True)
                    #filter out columns we don't need
                    if date > "2020-06":
                        df_tmp = df_tmp[self.nrpzs_cols_new]
                    else: #entries before august 2020 (including) have different column names
                        df_tmp = df_tmp.rename(columns={'KrajCode':'KrajKod', 'OkresCode':'OkresKod'})
                        df_tmp = df_tmp[self.nrpzs_cols_new]

                    #add info about month in which the data was retrieved
                    df_tmp['retrieved'] = date

                    #append new providers
                    # merged = pd.merge(df_tmp['ZdravotnickeZarizeniId'], nrpzs_df, on=['ZdravotnickeZarizeniId'], how='outer', indicator=True)
                    # nrpzs_df = nrpzs_df.append(df_tmp.loc[merged['_merge']=='left_only'], ignore_index=True)
                    # print(date, nrpzs_df.shape)

                    nrpzs_data.append(df_tmp)
                    print(df_tmp.shape)

            #get data from czso
            r = s.get(self.czso_data_url)
            r.raise_for_status()
            decoded_content = r.content.decode('utf-8')
            czso_df = pd.read_csv(StringIO(decoded_content), dtype=str, sep=',', quotechar='"', header=0, skipinitialspace=True)
            czso_df = czso_df.loc[czso_df['vuzemi_cis']=='100']
            czso_df = czso_df[self.czso_cols]
        
        return nrpzs_data, czso_df

def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = f"mongodb+srv://{os.environ.get('MONGODB_USERNAME')}:{os.environ.get('MONGODB_PASSWD')}@xkocal00-upa-2021.0z2ty.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    print(CONNECTION_STRING)

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

def import_df_to_mongo(df, db, coll_name, clear=False):
    coll = db[coll_name]
    payload = json.loads(df.to_json(orient='records'))
    coll.remove()
    coll.insert(payload)
    return coll.count()

def insert_df_to_mongo(df, db, coll_name):
    coll = db[coll_name]
    payload = df.to_dict(orient='records')
    coll.insert_many(payload)

def parseArguments():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--local', action='store_true',
                        help='Save downloaded data locally instead of uploading to database')
    parser.add_argument('--load', required=False,
                        help='Load data from folder instead of uploading to database')
    parser.add_argument('--drop-db', action='store_true',
                        help='Drop databases before import')

    return parser.parse_args()

def main():
    args = parseArguments()

    db = get_database()
    if args.drop_db:
        db['nrpzs'].drop()
        db['czso'].drop()

    if args.load:
        ...
    else:
        downlader = DataDownloader()
        data = downlader.download_data()
    
    if args.local:
        ...
    else:
        #cut into collections

        #load data into database
        for i, df in enumerate(data[0]):
            print(f"{datetime.now().strftime('%H:%M:%S')}|Importing data from {df['retrieved'].iloc(0)} into database")
            n = insert_df_to_mongo(df, db, "nrpzs")
        print(f"Imported {n} documents to collection nrpzs")
        print(f"{datetime.now().strftime('%H:%M:%S')}|Importing data czso data into database")
        n = insert_df_to_mongo(data[1], db, "czso")
        print(f"Imported {n} documents to collection czso")

        #create indexes
        db["nrpzs"].create_index('retrieved')



if __name__ == '__main__':
    main()