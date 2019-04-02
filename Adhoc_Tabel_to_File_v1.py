"""

|----------------------------------------------------------------------------------------------------------------------------|
|Created on Wed Mar 29 13:30:23 2019
|
|@author: vavashishtha
|This Utility reads a csv data file and a csv mapping document and loads data into a table by using these two inputs.
|
|Arguments Required in given order :
|[1] Target_Table_name, [2] Mapping Document in csv format
|
|Specifications for Target_Table_name : Please mention file name with proper schema
|
|
|Python calling Script example : Adhoc_Tabel_to_File_v1.py 'Schema_name.Table_name' 'Mapping_Docuemnt.csv'
|----------------------------------------------------------------------------------------------------------------------------|

"""


import sys
import cx_Oracle
import pandas as pd
import datetime
import csv

#Target_Table_name = sys.argv[1]
#Mapping_name= sys.argv[2]
Target_Table_name='ETLAPP.JOB_STATS'
dir_name='C:/Users/vavashishtha/OneDrive - PayPal/Python_Table_to_File/'
Mapping_name='Mapping_Doc.csv'
Final_file_name='output.csv'


#now = datetime.datetime.now()
#Prepared_date=str(now.strftime("%Y%m%d"))+'0001'
#df_excel = pd.read_csv(file_name,keep_default_na=False)
df_Mapping=pd.read_csv(Mapping_name)
#df_excel['Extract_ID']=Prepared_date
#df_excel['Row_Insert_TS']=now
#df_excel['NaN']=float('NAN')
#df_excel['Source_Row_Seq'] = range(1, 1+len(df_excel))
#Excel_column_list=list(df_excel)
Source_Column_Name=df_Mapping['Source_Target_Column_Name'].tolist()
Target_Column_Name=df_Mapping['Target_File_Column_Name'].tolist()

def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);


#Sql_select_statement=
Sql_execute="Select "
for j in Source_Column_Name:
    Sql_execute=Sql_execute+' to_char('+ str(j)+"), "

Sql_execute=Sql_execute[:-2]


Sql_execute=Sql_execute+" from "+Target_Table_name+" where to_date(RUN_DATE,'YYYYMMDD')=to_date('20120326','YYYYMMDD')"

databaseName = "DATATREK"
#print('vaibhav')
conn_str = 'etlapp/etlappstg@lvsvmdb81.qa.paypal.com:2126/QADBAA9P'
#conn_str = 'etlapp/Et1$ppJDEV654456@janatadev'
#conn_str = 'etlapp/asdfgcvi_23G@janataqa'


try:
    conn = cx_Oracle.connect(conn_str)
except cx_Oracle.DatabaseError as exception:
  printf ('Failed to connect to %s\n',databaseName)
  printException (exception)
  exit(0)


c = conn.cursor()


print("Selection will start Now,")
"""
try:
   c.execute(Sql_execute)

except cx_Oracle.DatabaseError as exception:
  print(Sql_execute)
  printf ('Failed to Insert into '+Target_Table_name+' ')
  printException (exception)


for errorObj in c.getbatcherrors():
    print("Row", errorObj.offset, " has error ", errorObj.message)

print('Selection has Ended')


outputFile = open(Final_file_name,'w') # 'wb'
output = csv.writer(outputFile, delimiter=',',quotechar='"', quoting=csv.QUOTE_ALL)
for row_data in c.fetchall(): # add table rows
  output.writerow(row_data)
  print(row_data)
"""



try:
        c.execute( Sql_execute )
        names = [ x[0] for x in c.description]
        rows = c.fetchall()
        df_oracle =pd.DataFrame( rows, columns=Target_Column_Name)
finally:
        if c is not None:
          conn.close()


print(df_oracle)


df_oracle.to_csv(Final_file_name,index=False)

print(df_oracle)