import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine

user = '#'
passw = '#'
host =  'mysql.kburchardt.com'  # either localhost or ip e.g. '172.17.0.2' or hostname address 
database = 'Rankings_Keywords'



mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host +  '/' + database , echo=False)

path = '/usr/local/airflow/dags/' ## airflow
os.chdir(path)


# -------exporting Keywords-------
csvFileName = os.path.join(path,'ranks.csv')
rank1 = pd.read_csv(os.path.join(csvFileName))
rank1.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Keywords
# csvFileName = 'keywords-all.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Urls
# csvFileName = 'urls-all.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)


# #-------exporting mobile-------
# csvFileName = 'mobile.csv'
# mobile = pd.read_csv(os.path.join(csvFileName))
# mobile.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Keywords
# csvFileName = 'keywords-mobile.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Urls
# csvFileName = 'urls-mobile.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)


# # #-------exporting desktop-------
# csvFileName = 'desktop.csv'
# desktop = pd.read_csv(os.path.join(csvFileName))
# desktop.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Keywords
# csvFileName = 'keywords-desktop.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Urls
# csvFileName = 'urls-desktop.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)



# # #-------exporting tablet-------
# csvFileName = 'tablet.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Keywords
# csvFileName = 'keywords-tablet.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)

# #exporting Urls
# csvFileName = 'urls-tablet.csv'
# tablet = pd.read_csv(os.path.join(csvFileName))
# tablet.to_sql(name=csvFileName[:-4], con=mydb, if_exists = 'replace', index=False)







"""
if_exists: {'fail', 'replace', 'append'}, default 'fail'
     fail: If table exists, do nothing.
     replace: If table exists, drop it, recreate it, and insert data.
     append: If table exists, insert data. Create if does not exist.
"""