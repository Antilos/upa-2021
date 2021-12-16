# plotB1.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question B1 - plot result

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from seaborn.categorical import barplot

csv_file = "queryB1.csv"
df = pd.read_csv(csv_file,index_col='vuzemi_kod')
#print(df)

df['PopPerDoc'] = df['PopCount'] / df['OborCount']
print(df)
df = df.sort_values(by=['PopPerDoc'])
print(df)

df['PopCount'] = df['PopCount'] / 1000
print(df)

oborCountList = df['OborCount'].to_list()
popCountList = df['PopCount'].to_list()
krajList = df['vuzemi_txt'].to_list()
popPerDocList = df['PopPerDoc'].to_list()

barWidth = 0.25
fig,ax = plt.subplots()
x1 = np.arange(len(krajList))
x2 = [x + barWidth for x in x1]
x3 = [x + 0.125 for x in x1]

plt.bar(x1, oborCountList, color ='b', width = barWidth,
        edgecolor ='grey', label ='Počet lékařů')
plt.bar(x2, popCountList, color ='g', width = barWidth,
        edgecolor ='grey', label ='Počet obyvatel (v tisících)')
plt.plot(x3, popPerDocList, color='r', label='Počet obyvatel na 1 lékaře')
#plt.xlabel('', fontweight ='bold', fontsize = 15)
#plt.ylabel('Počet poskytovatelů', fontweight ='bold', fontsize = 15)
# Adding Xticks
plt.xticks([x + barWidth/2 for x in range(len(krajList))],
        krajList, rotation='vertical')
plt.legend()
plt.tight_layout()
plt.grid(axis='y')
plt.title('Kraje dle počtu obyvatel na jednoho praktického lékaře')
plt.show()
