import mysql.connector
import pandas as pd

# Goal: Extract data on DMV drivers from 2010-2017

# Creating licensed driver data frame from U.S Licensed Driver Data

driversdata = pd.read_csv("/Users/buahinak/Downloads/Licensed_Drivers__by_state__gender__and_age_group.csv")
driverdf = pd.DataFrame(driversdata)
print(driverdf.columns)

# Connection to MySQL

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Compl3xity12!",
    database = "DMV_Drivers"
)

# Create DMV_Drivers Database(executed)

curs = connection.cursor()
create_db = 'CREATE DATABASE DMV_Drivers'
#curs.execute(create_db)

# Create tables for DC Maryland and Virginia
table_names = ['dc_drivers', 'md_drivers', 'va_drivers']
for table in table_names:
    createtable_query = """CREATE TABLE {} (
                      year INTEGER(10) NOT NULL,
                      gender varchar(30),
                      ages varchar(30),
                      state varchar(30),
                      drivers INTEGER NOT NULL)""".format(table)
    curs.execute(createtable_query)


# Creating DC, MD and VA Data frame from 2010-2018

dc_data = driverdf.loc[(driverdf.Year>=2010) & (driverdf.State == 'District of Columbia')]
dc_data.sort_values(by='Year')

va_data = driverdf.loc[(driverdf.Year>=2010) & (driverdf.State == 'Virginia')]
va_data.sort_values(by='Year')

md_data = driverdf.loc[(driverdf.Year>=2010) & (driverdf.State == 'Maryland')]
md_data.sort_values(by='Year')

# Inserting data frames into appropriate tables in SQL database

for row in dc_data.itertuples():
    insert_dc = """INSERT INTO DMV_Drivers.dc_drivers ( 
                year, gender, ages, state, drivers)
                VALUES (%s, %s, %s, %s, %s)
               """
    entry = (row.Year, row.Gender, row.Cohort, row.State, row.Drivers)
    curs.execute(insert_dc,entry)

for row in va_data.itertuples():
    insert_va = """INSERT INTO DMV_Drivers.va_drivers ( 
                year, gender, ages, state, drivers)
                VALUES (%s, %s, %s, %s, %s)
               """
    entry = (row.Year, row.Gender, row.Cohort, row.State, row.Drivers)
    curs.execute(insert_va,entry)

for row in md_data.itertuples():
    insert_md = """INSERT INTO DMV_Drivers.md_drivers ( 
                year, gender, ages, state, drivers)
                VALUES (%s, %s, %s, %s, %s)
               """
    entry = (row.Year, row.Gender, row.Cohort, row.State, row.Drivers)
    curs.execute(insert_md,entry)

# Commit changes and close connection/cursor
connection.commit()
connection.close()
curs.close()


