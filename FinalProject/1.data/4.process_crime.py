import pandas as pd
import numpy as np
from datetime import datetime

file_name = 'Crimes_2017.csv'
df = pd.read_csv(file_name)
#print(df.head(10))

#print(df.columns.values)

print("Select")
df_select = df[['ID','Case Number','Date','Block','Primary Type','Description','Location Description','Arrest','Community Area',]]

df_convert_month = df_select

print("Get DT")
dt = pd.DatetimeIndex(pd.to_datetime(df_select['Date']))

print("Creating new Cols")

df_convert_month['Month'] = dt.strftime("%m")
df_convert_month['Year'] = dt.strftime("%Y")
df_convert_month['Day'] = dt.strftime("%d")

#print(df_convert_month.head(10))

del df_convert_month['Date']
print("Save")

df_convert_month.to_csv(file_name.replace(".csv","")+"_processed.csv", sep=',',index=False,header=False)