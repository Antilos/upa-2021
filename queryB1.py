# queryB1.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question B1 - get data from DB

import csv
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
czso = dbaux['czso']

result0 = nrpzs.find_one(sort=[("retrieved",-1)])
newestDate = result0['retrieved']
obor = 'v\x9aeobecné praktické lékařství'

result1 = nrpzs.aggregate( [
    { '$match': {'OborPece':obor, 'retrieved':newestDate}},
    { '$group': { '_id':'$KrajKod', 'OborCount': { '$sum': 1 } } }
] )
df1 = DataFrame(list(result1))
#print(df1)

df1_1 = replaceKrajKod(df1,krajDict)
#print(df1_1)

krajList = list(krajDict.values())

result2 = czso.aggregate([
    { '$match': {'vuzemi_kod':{'$in':krajList},'vek_kod':{'$gt': "410015610020000"},'pohlavi_kod':{'$nin':['1','2']}}},
    { '$group': { '_id':'$vuzemi_kod','PopCount': {'$sum': {'$toInt':'$hodnota'}}}}
])
df2 = DataFrame(list(result2))

df3 = merge(df1_1,df2,how='outer',on='_id')
df3 = df3.rename(columns={"_id": "vuzemi_kod"})

result4 = czso.aggregate([{'$group':{'_id':{'vuzemi_kod':'$vuzemi_kod','vuzemi_txt':'$vuzemi_txt'}}}])
mytempList = (list(result4))
mytempDir = {}
for item in mytempList:
    itemID = item['_id']
    mytempDir[itemID['vuzemi_kod']]=itemID['vuzemi_txt']
#print(mytempDir)
s4 = Series(mytempDir,name='vuzemi_txt')
s4.index.name = 'vuzemi_kod'
df4 = merge(df3,s4,on='vuzemi_kod')
#print(df4)

csv_file = "queryB1.csv"
df4.to_csv(csv_file)
