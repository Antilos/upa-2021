# queryA1.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question A1

from pandas import DataFrame
from pandas import concat
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string

CONNECTION_STRING = f"mongodb+srv://xkocal00:xkocal00_upa_2021@xkocal00-upa-2021.0z2ty.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
#print(CONNECTION_STRING)
client = MongoClient(CONNECTION_STRING)
db=client.admin
# Issue the serverStatus command and print the results
serverStatusResult=db.command("serverStatus")
#pprint(serverStatusResult)

dbupa = client['upa']
nrpzs = dbupa['nrpzs']
#aux = nrpzs.find_one()
#print(aux) 
#aux = nrpzs.find({'OkresKod': 'CZ0100'})
#ux = nrpzs.find({'OkresKod': 'CZ0100'},{'DatumZahajeniCinnosti': 0})
#aux = nrpzs.find({'KrajKod': 'CZ064'},{'DatumZahajeniCinnosti': 0}).limit(10)
#print(aux.count())
#i = 0
#for x in aux:
#    if i < 10:
#        print(x)
#        i = i+1
#    else:
#        break

result0 = nrpzs.find_one(sort=[("retrieved",-1)])
newestDate = result0['retrieved']
#print(newestDate)

#result0 = nrpzs.find({'KrajKod': 'CZ064', 'OkresKod':{'$in':['CZ0642','CZ0643']},'retrieved':newestDate}).limit(10)
#for a in result0:
#    print(a)
#df0 = DataFrame(list(result0))
#print(df0)

result1 = nrpzs.aggregate( [
    { '$match': {'KrajKod': 'CZ064', 'OkresKod':{'$in':['CZ0642','CZ0643']},'retrieved':newestDate}},
    { '$group': { '_id': "$OborPece", 'OborCount': { '$sum': 1 } } }    
] ) # { '$match': { 'OborCount': { '$gte': 10*1000*1000 } } }
# alternativne match na obory uz v aggragate

df1 = DataFrame(list(result1))
df1['isBrno'] = 'yes'
#print(df1)

#list1 = []
#for a in result1:
#    a['isBrno'] = 'yes'
#    list1.append(a)
#    #if a['_id'].find('\x9a') != -1:
#    #    a['_id'] = a['_id'].replace('\x9a','s')
#    pprint(a)
#    #print(a['OborCount'])

# another query to get for the rest of jihomor kraj
result2 = nrpzs.aggregate( [
    { '$match': {'KrajKod': 'CZ064', 'OkresKod':{'$nin':['CZ0642','CZ0643']},'retrieved':newestDate}},
    { '$group': { '_id': "$OborPece", 'OborCount': { '$sum': 1 } }},
    { '$project': {'_id':1, 'OborPece':1,'OborCount':1,'retrieved':1}}    
] ) # { '$match': { 'OborCount': { '$gte': 10*1000*1000 } } }

df2 = DataFrame(list(result2))
df2['isBrno'] = 'no'
#print(df2)

#list2 = []
#for a in result2:
#    a['isBrno'] = 'no'
#    s = a['_id']
#    #if s.find('\x9a') != -1:
#    #    print(s)
#    #    s = s.replace('\x9a','š')
#    #    print(s)
#    #if a['_id'].find('\x9a') != -1:
#    #    a['_id'] = a['_id'].replace('\x9a','s')
#    list2.append(a)
#    #pprint(a)
#    #print(a['OborCount'])

#list1 = list(result1)
#print('list1:')
#print(list1)
#list2 = list(result2)
#list3 = list1+list2
#print('list3:')
#print(list3)
#df = DataFrame(list3)
df = concat([df1,df2])
#print(df.head())
df = df.rename(columns={'':'id','_id':'OborPece','OborCount':'OborCount','isBrno':'isBrno'})
#print(df)

#csv_columns = ['_id','OborPece','OborCount','isBrno']
csv_file = "queryA1.csv"
df.to_csv(csv_file)
