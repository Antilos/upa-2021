# plotA2.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question A2

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os.path

csv_file = "queryA2.csv"
if not os.path.exists(csv_file):
    print('csv file not found')
    exit()
df = pd.read_csv(csv_file)
#print(df)

fig,ax = plt.subplots()

oborlist = df.OborPece.unique()
dateArr = df.retrieved.unique()
# values for last month are messed up
dateArr = np.delete(dateArr, np.where(dateArr == '2021-11'))
dateArr = np.sort(dateArr)
#print(oborlist)
#print(dateArr)
xvals = np.arange(len(dateArr))
oborCnt = len(oborlist)
oborCntList = []

for obor in oborlist:
    auxdf = df[df['OborPece'] == obor]
    auxdf = auxdf.sort_values(by=['retrieved'])
    #print(auxdf)
    # values for last month are messed up
    auxdf = auxdf[auxdf['retrieved'] != '2021-11']
    tempList = auxdf['OborCount'].to_list()
    ax.plot(xvals,tempList,label=obor)

plt.xticks(xvals,dateArr,rotation='vertical')
plt.legend(loc="best") 
plt.title("Historie počtu poskytovatelů ve zvolených oborech")
plt.ylabel('Počet poskytovatelů')
plt.xlabel('Datum')
plt.show()
