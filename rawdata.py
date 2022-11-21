import cx_Oracle
import pandas as pd
import schedule
import time

#Create test connection to OracleDB
conn = cx_Oracle.connect('system/Oracle@123@localhost:1521/xe')
print(conn.version)

#create cursor
cur = conn.cursor()

#Create table 
sql_create = """
CREATE TABLE Employee_DETAILS(
    FIRST_NAME VARCHAR2(50),
    LAST_NAME VARCHAR2(50),
    COMPANY VARCHAR2(50),
    AGE NUMBER
)
"""
cur.execute(sql_create)
print('Table created')

try:
    #create connection

    try:
       curl = conn.cursor()
       sql_insert = """ INSERT INTO Employee_DETAILS VALUES ('Steave', 'Jobs', 'Apple', 56)"""    #Insert data 
       curl.execute(sql_insert)
    except Exception as err:
       print ('Error while inserting data ', err)
    else:
        print('Insert Completed.')
        conn.commit()
        
    try:
       curl = conn.cursor()
       sql= """ SELECT * FROM Employee_DETAILS """                      #Fetch the data 
       curl.execute(sql)
       row = cur.fetchall()
       print (row)
       
       for index, record in enumarate(row):
           print('Index is ', index, ' : ', record)
    except Exception as err:
       print('Exception occured while fetching the records', err)
    else
       print('Completed.') 
    finally:
       cur.close()         
    
    #run sql query

    sql_query = pd.read_sql_query(""'select * from Employee""", conn)

    #Writw the dataframe to csv file

    sql_query.to.csv (r'D:\D\python\Employee_data.csv', index = False)   #Export data into csv formart
               
finally:

    conn.close()
    
    
def schedule():                                                      #Scheduling to run the script everyday at  12AM
    print("execute the everydat at 00:00")
    
schedule.every().day.at("00:00").do(schedule)

while 1:
    schedule.run_pending()
    time.sleep(1)
    
    

    
    
    
    
    
    