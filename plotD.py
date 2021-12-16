# plotD.py
# Jiří Žilka (xzilka11)
# UPA 2021/2022
# question D - plot result

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from seaborn.categorical import barplot

csv_file = "queryD.csv"
newdf = pd.read_csv(csv_file)
print(newdf)
veklist=newdf['vek'].to_list()

bar_plot = barplot(x='muzi', y='vek', color='blue', data=newdf, order=veklist, lw=0)
bar_plot = barplot(x='zeny', y='vek', color='red', data=newdf, order=veklist, lw=0)
plt.title('Populační pyramida')
plt.xlabel('Počet obyvatel ve věkové skupině (vlevo muži, vpravo ženy)')
plt.vlines(x = 0, ymin=-0.5,ymax=19.5,color = 'black')
plt.show()
