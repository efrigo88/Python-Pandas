import pandas as pd
import sqlite3 as db
import os

# import data from .csv to a DataFrame
renewable_energy = pd.read_csv('files/renewable-energy-stock-account-2007-18.csv')

# print(renewable_energy.head(10))

# SQLite connection creation
con = db.connect('renewable_energy_database.db')

# delete unnecessary column from the DF
del renewable_energy['flag']

# write DataFrame into new SQLite table
renewable_energy.to_sql("renewable_energy_data", con, if_exists="replace", index=False)

# value by year and resource - ((calculation))
value_by_year = renewable_energy.groupby(['year', 'resource'])['data_value']\
    .sum()\
    .reset_index()\
    .sort_values(['year', 'resource'])

# to sqlite
value_by_year.to_sql("value_by_year", con, if_exists="replace", index=False)

#####################
# file output section
filename = 'renewable_energy.json'
# to json file
# If file exists, delete it
if os.path.isfile(filename):
    os.remove(filename)
renewable_energy.to_json(filename, orient='records', lines=True)

filename = 'value_by_year.json'
# to json file
# If file exists, delete it
if os.path.isfile(filename):
    os.remove(filename)
value_by_year.to_json(filename, orient='records', lines=True)




