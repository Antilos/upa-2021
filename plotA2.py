# plotA2.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022

from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from seaborn.categorical import barplot
import os.path

csv_file = "queryA2.csv"
if not os.path.exists(csv_file):
    # TODO execute query?
    pass
    # if still not exists then
    print('csv file not found')
    exit()
df = pd.read_csv(csv_file)#,index_col='OborPece'
print(df)

fig,ax = plt.subplots()

oborlist = df.OborPece.unique()
dateArr = df.retrieved.unique()
# values for last month are messed up TODO
dateArr = np.delete(dateArr, np.where(dateArr == '2021-11'))
dateArr = np.sort(dateArr)
print(oborlist)
print(dateArr)
xvals = np.arange(len(dateArr))
oborCnt = len(oborlist)
oborCntList = []

for obor in oborlist:
    auxdf = df[df['OborPece'] == obor]
    auxdf = auxdf.sort_values(by=['retrieved'])
    print(auxdf)
    # values for last month are messed up TODO
    auxdf = auxdf[auxdf['retrieved'] != '2021-11']
    tempList = auxdf['OborCount'].to_list()
    ax.plot(xvals,tempList,label=obor)

plt.xticks(xvals,dateArr,rotation='vertical')
plt.legend()
plt.title("Historie počtu poskytovatelů ve zvolených oborech")
plt.xlabel('Počet poskytovatelů')
plt.ylabel('Datum')
plt.show()
#sns.lineplot(x='retrieved',y='OborPece',data=df)
#plt.show()
exit()
#OborPeceList = ['Fyzioterapeut','Zubní technik','hematologie a transfúzní lékařství','Dentální hygienistka',
#'dermatovenerologie','kardiologie','radiologie a zobrazovací metody','angiologie','pneumologie a ftizeologie',
#'endokrinologie a diabetologie','Optometrista','chirurgie','vnitřní lékařství',
#'klinická biochemie','dětské lékařství'] # choose 15 OborPece values
#xpoints = np.array(OborPeceList)
#df2 = df.loc[OborPeceList]
#print(df2)

"""
barWidth = 0.25
fig,ax = plt.subplots()#figsize =(12, 8)
x1 = np.arange(len(OborPeceList))
x2 = [x + barWidth for x in x1]

plt.bar(x1, OborCountsYB, color ='b', width = barWidth,
        edgecolor ='grey', label ='Brno')
plt.bar(x2, OborCountsNB, color ='r', width = barWidth,
        edgecolor ='grey', label ='Zbytek')
 
# Adding Xticks
plt.xlabel('Obor', fontweight ='bold', fontsize = 15)
plt.ylabel('Počet poskytovatelů', fontweight ='bold', fontsize = 15)
plt.xticks([x + barWidth/2 for x in range(len(OborPeceList))],
        OborPeceList, rotation='vertical')
plt.legend()
plt.tight_layout()
plt.grid(axis='y')
plt.show()
"""

fig,ax = plt.subplots()
barWidth = 0.25
y1 = np.arange(len(OborPeceList))
y2 = [y + barWidth for y in y1]
ax.barh(y1, OborCountsYB, height=barWidth,label="Brno")
ax.barh(y2, OborCountsNB, height=barWidth,label="Zbytek")
 
plt.ylabel("Počet poskytovatelů")
plt.xlabel("Obor")
plt.yticks([y + 0.125 for y in range(len(OborPeceList))],
        OborPeceList)
plt.title("Počty poskytovatelů oborů v Jihomoravském kraji")
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.grid(axis = 'x')
plt.tight_layout()
plt.legend()
plt.show()
