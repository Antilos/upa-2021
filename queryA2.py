# queryA2.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question A2

from pandas import DataFrame
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

# choose 5 OborPece values
obory = ['Optometrista','dětské lékařství','hematologie a transfúzní lékařství',
        'Dentální hygienistka','kardiologie']

result1 = nrpzs.aggregate( [
    { '$match': {'OborPece':{'$in':obory}}},
    { '$group': { '_id': {'OborPece':'$OborPece','retrieved':'$retrieved'}, 'OborCount': { '$sum': 1 } } },
    { '$project': {'OborPece':1,'retrieved':1,'OborCount':1}}    
] )

doFlatten = True
if doFlatten == False: # no result modification
    auxlist = list(result1)
    print("auxlist:")
    print(auxlist)
else: # divide _id to OborPece,retrieved
    auxlist = []
    for a in result1:
        out = {'OborPece':a['_id']['OborPece'],'retrieved':a['_id']['retrieved'],'OborCount':a['OborCount']}
        #print(out)
        auxlist.append(out)

df = DataFrame(auxlist)

csv_file = "queryA2.csv"
df.to_csv(csv_file)
