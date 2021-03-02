import pandas as pd
import sqlite3

# SQLite connection creation
con = sqlite3.connect('testdatabase.db')

# get a cursor
cursor = con.cursor()

# execute a drop query
DropTableStatement = 'DROP TABLE test_join2'
cursor.execute(DropTableStatement)

# execute create table query
CreateTableScript = 'CREATE TABLE new_table (column1 int null, column2 int null, column3 int null);'
cursor.execute(CreateTableScript)

# close connection
con.close()
