# queryA1.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question A1

from pandas import DataFrame
from pandas import concat
from pymongo import MongoClient
from pprint import pprint

# connect to MongoDB
CONNECTION_STRING = f"mongodb+srv://xkocal00:xkocal00_upa_2021@xkocal00-upa-2021.0z2ty.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = MongoClient(CONNECTION_STRING)
db=client.admin
# Issue the serverStatus command and print the results
serverStatusResult=db.command("serverStatus")
#pprint(serverStatusResult)

dbupa = client['upa']
nrpzs = dbupa['nrpzs']

result0 = nrpzs.find_one(sort=[("retrieved",-1)])
newestDate = result0['retrieved']

#query to get data for Brno
result1 = nrpzs.aggregate( [
    { '$match': {'KrajKod': 'CZ064', 'OkresKod':{'$in':['CZ0642','CZ0643']},'retrieved':newestDate}},
    { '$group': { '_id': "$OborPece", 'OborCount': { '$sum': 1 } } }    
] ) 

df1 = DataFrame(list(result1))
df1['isBrno'] = 'yes'

# another query to get data for the rest of jihomor kraj
result2 = nrpzs.aggregate( [
    { '$match': {'KrajKod': 'CZ064', 'OkresKod':{'$nin':['CZ0642','CZ0643']},'retrieved':newestDate}},
    { '$group': { '_id': "$OborPece", 'OborCount': { '$sum': 1 } }},
    { '$project': {'_id':1, 'OborPece':1,'OborCount':1,'retrieved':1}}    
] ) 

df2 = DataFrame(list(result2))
df2['isBrno'] = 'no'

df = concat([df1,df2])
df = df.rename(columns={'':'id','_id':'OborPece','OborCount':'OborCount','isBrno':'isBrno'})

csv_file = "queryA1.csv"
df.to_csv(csv_file)
