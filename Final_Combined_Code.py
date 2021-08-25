# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 10:22:17 2021

@author: ijr
"""
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime
from meteostat import Point, Daily
from sklearn.preprocessing import StandardScaler
#from lazypredict.Supervised import LazyRegressor
from sklearn.model_selection import train_test_split



def numbercount(x):
    number = x.split('-')[0][3]
    return number

def check_col_float(col_name):
    result = []
    for i in tqdm(merge_df[col_name]):
        if not isinstance(i, float):
            result.append(i)
    return len(result)

def create_map(value):
    def Ammonia_map(x):
        try:
            return float(x)
        except:
            return value
    return Ammonia_map

def create_map2(value):
    def col_map(x):
        try:
            return float(x)
        except:
            return value
    return col_map

def date_split(x):
    return x.split()[0]

#Create wine_type and wine_number column
def lotconvert(x):
    split_str = x.split('-')[0]
    if split_str[3] == '0':
        return 'Red'
    elif split_str[3] == '1':
        return 'White'
    else:
        return 'Unknown'

__file__ = r'C:\Users\ijr\Documents\School Files\Capstone\Code'
abspath = os.path.abspath(__file__)
os.chdir(abspath)
import Hana_Connection as hana_conn
#directory = abspath
#names = ['Ammonia.csv', 'Glucose.csv', 'NOPA.csv', 'ph.csv']

data = hana_conn.main()
names = data[0:4]
#########
##Merge NOPA, glucose, ammonia (Deng's code)
#Merge all csv files
merge_df = pd.DataFrame()
for name in names:
    #print(name)
    #temp_path = os.path.join(directory, name)
    #df = pd.read_csv(temp_path)
    df = pd.DataFrame(name)
    new_col_nm =df.PL_ID[0]
    df = df[['SAMPLE_ID','S_CREATED_DATE', 'LOT', 'P_VALUE', 'EQUIPMENT_ID']]
    df.rename(columns = {'P_VALUE': f'{new_col_nm}_P_VALUE'}, inplace = True)
    if len(merge_df) == 0:
        merge_df = df
    else:
        merge_df = merge_df.merge(df, how = 'inner', on = ['SAMPLE_ID', 'S_CREATED_DATE', 'LOT', 'EQUIPMENT_ID'])

merge_df['Wine_number'] = list(map(numbercount, merge_df['LOT']))
merge_df['Wine_type'] = list(map(lotconvert, merge_df['LOT']))

merge_df['Ammonia_P_VALUE'] = list(map(create_map(13), merge_df['Ammonia_P_VALUE']))
merge_df['Glucose_P_VALUE'] = list(map(create_map(0.05), merge_df['Glucose/Fructose_P_VALUE']))
merge_df['NOPA_P_VALUE'] = list(map(create_map(5), merge_df['NOPA_P_VALUE']))

p_value_col = ['Ammonia_P_VALUE', 'Glucose/Fructose_P_VALUE', 'NOPA_P_VALUE', 'pH_P_VALUE']

for col in p_value_col:
    if check_col_float(col) == 0:
        print(f'All value in {col} are float')

# Split Created_date Column
merge_df['Year_Month_Day'] = list(map(date_split, merge_df['S_CREATED_DATE']))

#Find temp data
start = datetime.strptime(merge_df['Year_Month_Day'].min(), "%Y-%m-%d")
end = datetime.strptime(merge_df['Year_Month_Day'].max(), "%Y-%m-%d")

latitude = 37.37527515893403
longitude = -120.81473687539952
livingston = Point(latitude, longitude)

temp_data = Daily(livingston, start, end)
temp_data = temp_data.fetch()

temp_data = temp_data.reset_index()[['time', 'tavg']]
temp_data['time'] = temp_data['time'].dt.strftime('%Y-%m-%d')
#temp_data.head()


final_df = merge_df.merge(temp_data, how = 'inner', left_on = 'Year_Month_Day', right_on = 'time')
final_df.tavg.fillna(round(final_df.tavg.mean(), 2), inplace = True)
final_df.isna().sum().sum()
#final_df.head()

final_df.to_csv('final_df.csv', index = False)

############################
#############################
#Merge SO2, malic, lactic (Deng's code)
original_df = final_df
files = data[4:8]

colname = []
direcotry = r'D:\DATA\capstone_data\so2'
for file in files:
    temp_df = pd.DataFrame(file)
    new_col_nm =temp_df.PL_ID[0]
    colname.append(f'{new_col_nm}_P_VALUE')
    #file_path = os.path.join(direcotry, file)
    #temp_df = pd.read_csv(file_path)
    temp_df = temp_df[['SAMPLE_ID', 'EQUIPMENT_ID', 'LOT', 'S_CREATED_DATE', 'P_VALUE']]
    temp_df.rename(columns = {'P_VALUE': f'{new_col_nm}_P_VALUE'}, inplace = True)
    original_df = original_df.merge(temp_df, how = 'left', on = ['SAMPLE_ID', 'S_CREATED_DATE', 'LOT', 'EQUIPMENT_ID'])

df = original_df.copy(deep = True)

df['Free SO2_P_VALUE'] = list(map(create_map2(2), df['Free SO2_P_VALUE']))
df['Lactic Acid_P_VALUE'] = list(map(create_map2(None), df['Lactic Acid_P_VALUE']))
df['Malic Acid_P_VALUE'] = list(map(create_map2(None), df['Malic Acid_P_VALUE']))
df['Total SO2_P_VALUE'] = list(map(create_map2(None), df['Total SO2_P_VALUE']))

df[colname] = df[colname].fillna(round(df[colname].mean(), 2))
df.isna().sum().sum()
#df.shape

df.to_csv('final_df_2.0.csv', index = False)

#################
##################### (Where I left off)
#Ingredience Table (Trang)
#combine data to get ingredience 
df = pd.read_csv("ingridientmaterial_data.csv")

df.head()
df2 = df.sort_values(by=["TNK_I","LOT_I","WRK_ORD_I"])
df2[["TNK_I","WRK_ORD_I","CMPLT_T","LOT_I","MaterialName"]].head(n = 10)
df3 = df2.set_index(["TNK_I","LOT_I","WRK_ORD_I","CMPLT_T"])
df3.head(n = 5)

count = df3['MaterialGroupName'].value_counts()
count

count = df3['MaterialName'].value_counts()
count.head(n = 10)


bigdf = final_df
bigdf.head()


#Left Join with Ingridient Dataset
ingridient = df2[["TNK_I","LOT_I","CMPLT_T","WRK_ORD_I", "MaterialName","MaterialTypeName", "MaterialGroupName", "MaterialWeightUnit", "MaterialNetWeight", "MaterialGrossWeight"]]
new_df = pd.merge(bigdf, ingridient,  how='left', left_on=['EQUIPMENT_ID','LOT'], right_on = ['TNK_I','LOT_I'])
new_df.tail()
new_df.isnull().any(axis = 1).sum()
new_df = new_df.groupby(['TNK_I','LOT_I','S_CREATED_DATE','CMPLT_T'])


# create a loop, go to each group, filter only row that has completed time happened before s_screated_time
#then find the latest completed time
data = []
for name, group in new_df:
    goal = group["CMPLT_T"] < group["S_CREATED_DATE"]
    new_group = group.loc[goal]
    new_group2 = new_group[(new_group['CMPLT_T'] == new_group['CMPLT_T'].max())] 
    if not new_group.empty:
        data.append(new_group2)

data[2]

# we have a list of 444 subsets, each subset is a tank with a lot with latest completed ingridient compositions
# right before sampling
len(data)
merged = pd.concat(data)
len(merged)
merged.head()

merged2 = merged
merged2.to_csv('finaldata_before_hotcode_03.csv', index=False)

# Find frequency of ingrdients composition in materialname column
count = merged['MaterialName'].value_counts()

# Filter the most frequent igridient material name
filter_merged = merged2[(merged2['MaterialName'] == 'ADDITIVE-SULFUR DIOXIDE BULK') |
          (merged2['MaterialName'] == 'ACID-TARTARIC ACID SUPERSACKS') |
          (merged2['MaterialName'] == 'ENZYME-ROHAVIN MX 25 KG AB ENZYMES') |
          (merged2['MaterialName'] == 'ENZYME-ROHAVIN L 25KG AB ENZYMES') |
          (merged2['MaterialName'] == 'FINING AID-U.S. GELATIN 55 LB') |
          (merged2['MaterialName'] == 'ADDITIVE-POTASSIUM METABISULF 99% 55LB') |
          (merged2['MaterialName'] == 'ADDITIVE-POT METABI 14.7% LIQ') |
          (merged2['MaterialName'] == 'FINING AID-SILICA GEL 2,700 LB  TOTE') |
          (merged2['MaterialName'] == 'CARBON-NUCHAR HD MAX BULK MEADWESTVACO') |
          (merged2['MaterialName'] == 'ENZYME-PECTINEX XXL 25KG') |
          (merged2['MaterialName'] == 'ACID-TARTARIC SUPERSACK 2204.6 BULK BAG') |
          (merged2['MaterialName'] == 'FINING AID-PORK GELATIN 175 PS 30') |
          (merged2['MaterialName'] == 'ENZYME-ROHALASE BXL AB ENZYMES') |
          (merged2['MaterialName'] == 'ENZYME-ROHAPECT VR-L 25KG') |
          (merged2['MaterialName'] == 'ADDITIVE-COPPER SULFATE RGNT 100LB DRUM') |
          (merged2['MaterialName'] == 'ACID-TARTARIC ACID 55LB BAG') |
          (merged2['MaterialName'] == 'YEAST-RAVAGO MAURIVIN ELEGANCE 10KG')|
          (merged2['MaterialName'] == 'NUTRIENT-DIAMMONIUM PHOSPHATE 55LB') |
          (merged2['MaterialName'] == 'FINING AID-VITIBEN BENTONITE') |
          (merged2['MaterialName'] == 'ENZYME-SUMIZYME MHT')]


filter_merged = filter_merged[['SAMPLE_ID','S_CREATED_DATE','LOT','CMPLT_T','WRK_ORD_I','Ammonia_P_VALUE','EQUIPMENT_ID','Glucose_P_VALUE','MaterialName', 'MaterialGroupName'
]]


df_all = pd.get_dummies(filter_merged, prefix=['MaterialName', 'MaterialGroupName'], columns=['MaterialName', 'MaterialGroupName'])
df_test2 = df_all.groupby(['SAMPLE_ID','S_CREATED_DATE','LOT','CMPLT_T','WRK_ORD_I','Ammonia_P_VALUE','EQUIPMENT_ID','Glucose_P_VALUE']).sum()

df_test2 = df_test2.reset_index()
df_test3 = df_test2.drop_duplicates(subset='SAMPLE_ID', keep='last')


sub_bigdf = bigdf[['SAMPLE_ID','S_CREATED_DATE','NOPA_P_VALUE', 'ph_P_VALUE', 'Wine_number',
       'Wine_type', 'Year_Month_Day', 'time', 'tavg', 'Free SO2_P_VALUE',
       'Lactic Acid_P_VALUE', 'Malic Acid_P_VALUE', 'Total SO2_P_VALUE']]


df_all2 = pd.merge(sub_bigdf, df_test3,  how='left', left_on=['SAMPLE_ID'], right_on = ['SAMPLE_ID'])
df_all2.drop(['S_CREATED_DATE_y'], axis = 1)

df_all2.to_csv('finaldata_after_hotcode_04.csv', index=False)

###################
#################
# Model (Asad's code)

df = final_df

# droping columns 
df = df.drop(['SAMPLE_ID', 'S_CREATED_DATE','LOT',
              'EQUIPMENT_ID', 'Year_Month_Day', 'time',
              'CMPLT_T','WRK_ORD_I','Wine_type','Year_Month_Day', 'time', 'tavg'], axis = 1)
X = df.drop(['Glucose_P_VALUE'], axis = 1)
y = df['Glucose_P_VALUE']


scale= StandardScaler()
X = scale.fit_transform(X)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Lazy predict calling 
reg = LazyRegressor(verbose=0, ignore_warnings=False, custom_metric=None)
models, predictions = reg.fit(X_train, X_test, y_train, y_test)

print(models)


