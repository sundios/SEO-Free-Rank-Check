import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine

user = 'username'
passw = 'password'
host =  'Host'  # either localhost or ip e.g. '172.17.0.2' or hostname address 
database = 'Rankings'

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host +  '/' + database , echo=False)

path = '/usr/local/airflow/dags/all_kws/' ## airflow
os.chdir(path)


def main():
	# -------exporting Kw1-------
	csvFileName = os.path.join(path,'kw1.csv')
	rank1 = pd.read_csv(os.path.join(csvFileName))
	rank1.to_sql(name='kw1', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw2
	csvFileName = os.path.join(path,'kw2.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw2', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw3
	csvFileName = os.path.join(path,'kw3.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw3', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw4
	csvFileName = os.path.join(path,'kw4.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw4', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw5
	csvFileName = os.path.join(path,'kw5.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw5', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw6
	csvFileName = os.path.join(path,'kw6.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw6', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw7
	csvFileName = os.path.join(path,'kw7.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw7', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw8
	csvFileName = os.path.join(path,'kw8.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw8', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw9
	csvFileName = os.path.join(path,'kw9.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw9', con=mydb, if_exists = 'replace', index=False)

	#exporting Kw10
	csvFileName = os.path.join(path,'kw10.csv')
	tablet = pd.read_csv(os.path.join(csvFileName))
	tablet.to_sql(name='kw10', con=mydb, if_exists = 'replace', index=False)

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

if __name__ == '__main___':
	main()



"""
if_exists: {'fail', 'replace', 'append'}, default 'fail'
     fail: If table exists, do nothing.
     replace: If table exists, drop it, recreate it, and insert data.
     append: If table exists, insert data. Create if does not exist.
"""