# question A1

CONNECTION_STRING = f"mongodb+srv://xkocal00:xkocal00_upa_2021@xkocal00-upa-2021.0z2ty.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
print(CONNECTION_STRING)

from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(CONNECTION_STRING)
db=client.admin
# Issue the serverStatus command and print the results
serverStatusResult=db.command("serverStatus")
#pprint(serverStatusResult)

dbaux = client['upa']
aux = dbaux['nrpzs'].find_one()
auxnrpzs = dbaux['nrpzs']
#print(aux) 
#aux = dbaux['nrpzs'].find({'OkresKod': 'CZ0100'})
#ux = dbaux['nrpzs'].find({'OkresKod': 'CZ0100'},{'DatumZahajeniCinnosti': 0})
aux = dbaux['nrpzs'].find({'KrajKod': 'CZ064'},{'DatumZahajeniCinnosti': 0}).limit(10)
print(aux.count())
i = 0
for x in aux:
    if i < 10:
        #print(x)
        i = i+1
    else:
        break



namesList = ['Fyzioterapeut','Zubní technik','hematologie a transfúzní lékařství','Dentální hygienistka',
'dermatovenerologie','kardiologie','v\x9aeobecné praktické lékařství','a','b','c','a','a','a','a','a'] # choose 15 OborPece values
import numpy as np
xpoints = np.arange(1,16)
vals = np.arange(1,16)
xpoints = np.array(['Fyzioterapeut','Zubní technik','hematologie a transfúzní lékařství','Dentální hygienistka',
'dermatovenerologie','kardiologie','v\x9aeobecné praktické lékařství','a','b','c','1','2','3','4','5'])
counts = []
#print(xpoints)
import matplotlib.pyplot as plt
plt.bar(xpoints,vals)
#plt.show()

dict1 = {}
result1 = auxnrpzs.aggregate( [
    { '$match': {'KrajKod': 'CZ064', 'OkresKod':{'$in':['CZ0642','CZ0643']},'retrieved':'2021-11'}},
    { '$group': { '_id': "$OborPece", 'OborCount': { '$sum': 1 } } }    
] ) # { '$match': { 'OborCount': { '$gte': 10*1000*1000 } } }
# alternativne match na obory uz v aggragate

list1 = []
for a in result1:
    a['isBrno'] = 'yes'
    list1.append(a)
    #if a['_id'].find('\x9a') != -1:
    #    a['_id'] = a['_id'].replace('\x9a','s')
    pprint(a)
    #print(a['OborCount'])

from pandas import DataFrame


#todo another query to get for the rest of jihomor kraj
result2 = auxnrpzs.aggregate( [
    { '$match': {'KrajKod': 'CZ064', 'OkresKod':{'$nin':['CZ0642','CZ0643']},'retrieved':'2021-11'}},
    { '$group': { '_id': "$OborPece", 'OborCount': { '$sum': 1 } }},
    { '$project': {'_id':1, 'OborPece':1,'OborCount':1,'retrieved':1}}    
] ) # { '$match': { 'OborCount': { '$gte': 10*1000*1000 } } }

dict2 = {}
list2 = []
for a in result2:
    a['isBrno'] = 'no'
    s = a['_id']
    #if s.find('\x9a') != -1:
    #    print(s)
    #    s = s.replace('\x9a','š')
    #    print(s)
    #if a['_id'].find('\x9a') != -1:
    #    a['_id'] = a['_id'].replace('\x9a','s')
    list2.append(a)
    #pprint(a)
    #print(a['OborCount'])

#list1 = list(result1)
print('list1:')
print(list1)
#list2 = list(result2)
list3 = list1+list2
print('list3:')
print(list3)
df = DataFrame(list3)
print(df.head())
df = df.rename(columns={'':'id','_id':'OborPece','OborCount':'OborCount','isBrno':'isBrno'})
print(df)

#csv_columns = ['_id','OborPece','OborCount','isBrno']
csv_file = "queryA1.csv"
df.to_csv(csv_file)
