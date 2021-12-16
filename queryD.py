# queryD.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question D - get data from DB

import csv
from re import match
from numpy.core.numeric import False_
from numpy.testing._private.utils import tempdir
from pymongo import MongoClient
from pprint import pprint
from pandas import DataFrame
from pandas import Series
from pandas import merge
from seaborn import barplot
from matplotlib import pyplot as plt

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
czso = dbaux['czso']

krajList = list(krajDict.values())
result = czso.aggregate([
    { '$match': {'vuzemi_kod':{'$in':krajList},'pohlavi_kod':{'$in':['1','2']}}},
    { '$group': { '_id':{'Pohlavi':'$pohlavi_kod','vek_kod':'$vek_kod',},'PopCount': {'$sum': {'$toInt':'$hodnota'}}}},
    { '$sort' : { '_id.Pohlavi':1,'_id.vek_kod':1}}
])
auxlist = []
for a in result:
    out = {'Pohlavi':a['_id']['Pohlavi'],'Vek_kod':a['_id']['vek_kod'],'PopCount':a['PopCount']}
    #print(out)
    auxlist.append(out)

df = DataFrame(list(auxlist))
#print(len(df))
df = df.dropna()
#print(len(df))
#print(df)
#print(df['PopCount'].sum()) # celkove populace

dfm = df[df['Pohlavi'] == '1']
dfm = dfm.sort_values(by='Vek_kod',ascending=False)
dff = df[df['Pohlavi'] == '2']
dff = dff.sort_values(by='Vek_kod',ascending=False)
#print(len(dfm))
#print(len(dff))
mCntlist = dfm['PopCount'].to_list()
fCntlist = dff['PopCount'].to_list()
veklist = ['0-4','5-9','10-14','15-19','20-24',
            '25-29','30-34','35-39','40-44','45-49','50-54',
            '55-59','60-64','65-69','70-74','75-79','80-84',
            '85-89','90-94','95-99'] # ,'100+'
veklist.reverse()
newdf = DataFrame({ 'vek': veklist, 
                    'muzi': mCntlist,
                    'zeny': fCntlist})
print(newdf)
newdf['zeny'] = newdf['zeny'].astype(int)
newdf['muzi'] = newdf['muzi'].astype(int)
newdf['muzi'] = newdf['muzi'] * (-1)
#print(newdf)

csv_file = "queryD.csv"
newdf.to_csv(csv_file)
