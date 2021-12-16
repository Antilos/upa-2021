# queryC2.py
# Jiří Žilka (xzilka11) Jakub Kočalka (xkocal00)
# UPA 2021/2022
# question C2 - get data from DB

import csv
from datetime import date
from re import match
from numpy.testing._private.utils import tempdir
from pandas.core.algorithms import factorize
from pymongo import MongoClient
from pprint import pprint
from pandas import DataFrame
from pandas import Series
from pandas import merge

def replaceKrajKod(df,myKrajDict):
    df = df.replace(to_replace=myKrajDict,value=None)
    return df

def getKrajDict():
    myKrajDict = {}
    try:
        with open('xzilkaKrajDict.csv', 'r') as f:
            for kraj in csv.reader(f):
                myKrajDict[kraj[1]] = kraj[0]
    except IOError:
        print('reading xzilkaKrajDict.csv: I/O error')
        return None
    return myKrajDict

krajDict = getKrajDict()
#print(krajDict)

CONNECTION_STRING = f"mongodb+srv://xkocal00:xkocal00_upa_2021@xkocal00-upa-2021.0z2ty.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
#print(CONNECTION_STRING)
client = MongoClient(CONNECTION_STRING)
db=client.admin
serverStatusResult=db.command("serverStatus")
#pprint(serverStatusResult)

dbaux = client['upa']
nrpzs = dbaux['nrpzs']

result0 = nrpzs.find_one(sort=[("retrieved",-1)])
newestDate = result0['retrieved']
obor = 'v\x9aeobecné praktické lékařství'

obory = nrpzs.distinct('OborPece')
obory20 = obory[6:26]
#print(len(obory20))
#print(obory20)

oborCountDictList = []
for obor in obory20:
    oborCountDict = {}
    #oborCountDict['obor'] = obor # TODO chceme si tohle ukladat?
    result = nrpzs.aggregate( [
        { '$match': {'OborPece':obor}},
        { '$group': { '_id': '$retrieved', 'OborCount': { '$sum': 1 }} },
        { '$sort' : {'_id':1}},
    ] )
    for a in result:
        oborCountDict[a['_id']] = a['OborCount']
    oborCountDictList.append(oborCountDict)   

df = DataFrame.from_records(oborCountDictList)
print(df)

csv_file = "queryC2.csv"
df.to_csv(csv_file)
