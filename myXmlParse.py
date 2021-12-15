# myXmlParse.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# create kraj & okres dictionary and save as csv

import xml.etree.ElementTree as ET
import csv

tree = ET.parse('CIS0101_CS.xml')
root = tree.getroot()
#print(root.tag)
data = root.find('DATA')
myDict = {}

for polozka in data.findall('POLOZKA'):
    chodnota = polozka.find('CHODNOTA').text
    #print(chodnota)
    atributy = polozka.find('ATRIBUTY')
    #print(atributy)
    for atr in atributy:
        #print(atr.get('akronym'))
        #print(atr.text)
        if atr.get('akronym') == "CZNUTS":
            #print(atr.text)
            #print(chodnota,' : ',atr.text)
            myDict[chodnota] = atr.text

#print(myDict)
csvName = 'xzilkaOkresDict.csv'
try:
    with open(csvName, 'w') as f:
        for key in myDict.keys():
            f.write("%s,%s\n"%(key,myDict[key]))
except IOError:
    print('io error')

# get kraj dictionary
tree = ET.parse('CIS0100_CS.xml')
root = tree.getroot()
data = root.find('DATA')
myDict = {}
for polozka in data.findall('POLOZKA'):
    chodnota = polozka.find('CHODNOTA').text
    atributy = polozka.find('ATRIBUTY')
    for atr in atributy:
        if atr.get('akronym') == "CZNUTS":
            myDict[chodnota] = atr.text

csvName = 'xzilkaKrajDict.csv'
try:
    with open(csvName, 'w') as f:
        for key in myDict.keys():
            f.write("%s,%s\n"%(key,myDict[key]))
except IOError:
    print('io error')
