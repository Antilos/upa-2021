# queryA2.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question A2

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
auxnrpzs = dbaux['nrpzs']

# choose 5 OborPece values
obory = ['Fyzioterapeut','Zubní technik','hematologie a transfúzní lékařství',
        'Dentální hygienistka','kardiologie']

# TODO match obory later or now?
result1 = auxnrpzs.aggregate( [
    { '$match': {'OborPece':{'$in':obory}}},
    { '$group': { '_id': {'OborPece':'$OborPece','retrieved':'$retrieved'}, 'OborCount': { '$sum': 1 } } },
    { '$project': {'OborPece':1,'retrieved':1,'OborCount':1}}    
] )

doFlatten = True
if doFlatten == False:
    auxlist = list(result1)
    print("auxlist")
    print(auxlist)
else:
    auxlist = []
    for a in result1:
        out = {'OborPece':a['_id']['OborPece'],'retrieved':a['_id']['retrieved'],'OborCount':a['OborCount']}
        print(out)
        auxlist.append(out)

from pandas import DataFrame
df = DataFrame(auxlist)
#print(df.head())

csv_file = "queryA2.csv"
df.to_csv(csv_file)
