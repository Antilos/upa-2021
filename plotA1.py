# plotA1.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question A1 - plot result

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

csv_file = "queryA1.csv"
df = pd.read_csv(csv_file,index_col='OborPece')

OborPeceList = ['Fyzioterapeut','Zubní technik','hematologie a transfúzní lékařství','Dentální hygienistka',
'dermatovenerologie','kardiologie','radiologie a zobrazovací metody','angiologie','pneumologie a ftizeologie',
'endokrinologie a diabetologie','Optometrista','chirurgie','vnitřní lékařství',
'klinická biochemie','dětské lékařství'] # choose 15 OborPece values

df2 = df.loc[OborPeceList]

OborCountsYB = []
OborCountsNB = []
for obor in OborPeceList:
    a = df.loc[obor]
    yb = a.where(a['isBrno']=='yes')
    yb = yb.dropna(axis='index') 
    OborCountsYB.append(int(yb['OborCount']))
    nb = a.where(a['isBrno']=='no') 
    nb = nb.dropna(axis='index')
    OborCountsNB.append(int(nb['OborCount']))
    
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
