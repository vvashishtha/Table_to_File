"""

|----------------------------------------------------------------------------------------------------------------------------|
|Created on Wed Mar 29 13:30:23 2019
|
|@author: vavashishtha
|This Utility reads a csv data file and a csv mapping document and loads data into a table by using these two inputs.
|
|Arguments Required in given order :
|[1] Target_Table_name, [2] Mapping Document in csv format, [3] Output file name in csv format
|
|Specifications for Target_Table_name : Please mention file name with proper schema
|
|
|Python calling Script example : Adhoc_Tabel_to_File_v1.py 'Schema_name.Table_name' 'Mapping_Docuemnt.csv'
|----------------------------------------------------------------------------------------------------------------------------|

"""

import numpy as np
import sys
import cx_Oracle
import pandas as pd
import datetime
import csv

#Target_Table_name = sys.argv[1]
#Mapping_name= sys.argv[2]+'.csv'
Target_Table_name='ETLAPP.JOB_STATS'
dir_name='C:/Users/vavashishtha/OneDrive - PayPal/Python_Table_to_File/'
Mapping_name='Mapping_Doc.csv'
Final_file_name='output.csv'
Column_filter='RUN_DATE'
Filter_value='20120326'



#now = datetime.datetime.now()
#Prepared_date=str(now.strftime("%Y%m%d"))+'0001'
#df_excel = pd.read_csv(file_name,keep_default_na=False)
#df_excel['Extract_ID']=Prepared_date
#df_excel['Row_Insert_TS']=now
#df_excel['NaN']=float('NAN')
#df_excel['Source_Row_Seq'] = range(1, 1+len(df_excel))
#Excel_column_list=list(df_excel)

Target_Table_name_list=Target_Table_name.split('.')
Schema_name=Target_Table_name_list[0]
Table_name=Target_Table_name_list[1]
print(Schema_name)
print(Table_name)


df_Mapping=pd.read_csv(Mapping_name)
df_Mapping=df_Mapping.dropna(subset=['Source_Target_Column_Name'])
df_Mapping=df_Mapping.fillna('')

Source_Column_Name=df_Mapping['Source_Target_Column_Name'].tolist()
Target_Column_Name=df_Mapping['Target_File_Column_Name'].tolist()

def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);


def sub_set(list1, list2):   
##""" function is boolean, returns True, if list1 is a super set  of list2 (if all list 2 elements are in list1 ) """
  result =  all(elem in list1  for elem in list2)
    
  if result:
      return True
  else :
      return False
def list_substract(list1, list2):  
  return ([item for item in list1 if item not in list2])
    
## Creating connections

databaseName = "DATATREK"
#print('vaibhav')
conn_str = 'etlapp/etlappstg@lvsvmdb81.qa.paypal.com:2126/QADBAA9P'
#conn_str = 'etlapp/Et1$ppJDEV654456@janatadev'
#conn_str = 'etlapp/asdfgcvi_23G@janataqa'

##--------------------------------------------------------------------------------------------------------------
## validation if the connection exists

try:
    conn = cx_Oracle.connect(conn_str)
except cx_Oracle.DatabaseError as exception:
  printf ('Failed to connect to %s\n',databaseName)
  printException (exception)
  exit(0)

##_----------------------------------------------------------------------------------------------------------------
## Creating the sql statement 

Sql_select_validate="Select distinct COLUMN_NAME from ALL_TAB_COLUMNS where upper(owner) like upper('"+Schema_name+"') and upper(table_name) like upper('"+Table_name+"')"
Sql_execute="Select "
Source_Column_Name1=Source_Column_Name
for i in range(len(Source_Column_Name1)):
    j=Source_Column_Name[i]
    if j == '':
        Target_Column_Name.pop(i)
        Source_Column_Name.pop(i)
    else:    
        Sql_execute=Sql_execute+' to_char('+ str(j)+"), "

## creation of connection for validation of input columns

cur=conn.cursor()


try:
        cur.execute( Sql_select_validate )
        names = [ x[0] for x in cur.description]
        rows = cur.fetchall()
        df_oracle1 =pd.DataFrame( rows, columns=names)
except cx_Oracle.DatabaseError as exception:
  printf ('Failed to find the columns in table : %s for schema : %s ',Table_name,Schema_name)
  printException (exception)

Col_available_table=df_oracle1['COLUMN_NAME'].tolist()
bol_sub_set=sub_set(Col_available_table,Source_Column_Name)

## if there is a missmatch of columns in mapping and table, we will abort the process

if bol_sub_set == False:
    print("All the Column names available in mapping document are not available in table,\n Please verify the column names")
    ## we print the column available in mapping doc but not in table
    print(list_substract(Source_Column_Name,Col_available_table)) 

    exit(0)
    
####---------------------------------------- actuall select-----------------------    

Sql_execute=Sql_execute[:-2]
Sql_execute=Sql_execute+" from "+Target_Table_name+" where to_date("+Column_filter+",'YYYYMMDD')=to_date('"+Filter_value+"','YYYYMMDD')"

c = conn.cursor()


print('Selection will start Now')

try:
        c.execute( Sql_execute )
        #cur.execute('DESC '+ Target_Table_name)
        #print(cur.fetchall())
        names = [ x[0] for x in c.description]
        rows = c.fetchall()
        df_oracle =pd.DataFrame( rows, columns=Target_Column_Name)
finally:
        if c is not None:
          conn.close()

print("selection has finished succssfully")
print("Inserting data into csv file now")
df_oracle.to_csv(Final_file_name,index=False)
print("Data succssfully inserted to csv file")