import pandas as pd
from matplotlib import pyplot as plt

# we import some data
airports = pd.read_csv('files/airports.csv')
airport_freq = pd.read_csv('files/airport-frequencies.csv')
runways = pd.read_csv('files/runways.csv')

# simple SELECT
# print(airports)

# SELECT LIMIT 3
test1 = airports.head(5)
# print(type(test1))

# SELECT id FROM airports
# WHERE ident = 'KLAX'
test2 = airports[airports.ident == 'KLAX'].id
# print(type(test2))

# SELECT DISTINCT type FROM airports
test3 = airports.type.unique()
# print(test3)

# SELECT * FROM airports
# WHERE iso_region = 'US-CA' AND type = 'seaplane_base'
test4 = airports[(airports.iso_region == 'US-CA') &
                 (airports.type == 'seaplane_base')]
# print(test4)

# SELECT ident, name, municipality FROM airports
# WHERE iso_region = 'US-CA' AND type = 'large_airport'
test5 = airports[(airports.iso_region == 'US-CA') &
                 (airports.type == 'seaplane_base')][['ident',
                                                      'name',
                                                      'municipality']]
# print(test5)

# select * from airport_freq
# where airport_ident = 'KLAX'
# order by type
test6 = airport_freq[airport_freq.airport_ident == 'KLAX'].sort_values('type')
# print(test6)

# select * from airport_freq
# where airport_ident = 'KLAX'
# order by type desc
test7 = airport_freq[airport_freq.airport_ident == 'KLAX'].sort_values('type', ascending=False)
# print(test7)

# select * from airports
# where type in ('heliport', 'balloonport')
test8 = airports[airports.type.isin(['heliport', 'balloonport'])]
# print(test8)

# select * from airports
# where type not in ('heliport', 'balloonport')
test9 = airports[~airports.type.isin(['heliport', 'balloonport'])]
# print(test9)

# select iso_country, type, count(*) from airports
# group by iso_country, type
# order by iso_country, type
test10 = airports.groupby(['iso_country', 'type']).size()
# print(test10)

# select iso_country, type, count(*) from airports
# group by iso_country, type
# order by iso_country, count(*) desc
test11 = airports.groupby(['iso_country', 'type']).size()\
    .to_frame('size')\
    .reset_index()\
    .sort_values(['iso_country', 'size'], ascending=False)
# print(test11)

# select type, count(*) from airports
# where iso_country = 'US'
# group by type
# having count(*) > 1000
# order by count(*) desc
test12 = airports[airports.iso_country == 'US']\
    .groupby('type').filter(lambda g: len(g) > 1000)\
    .groupby('type').size().sort_values(ascending=False)
# print(test12)

# we create the data frame with necessary data to carry on with the practice
by_country = airports.groupby('iso_country').size()\
    .to_frame('airport_count')\
    .reset_index()
# print(by_country.head(3))

# select iso_country from by_country
# order by size desc limit 10
test13 = by_country.nlargest(10, columns='airport_count')
# print(test13)

# select iso_country from by_country
# order by size desc
# limit 10 offset 10
test14 = by_country.nlargest(20, columns='airport_count').tail(10)
# print(test14)

# select max(length_ft), min(length_ft), avg(length_ft), median(length_ft)
# from runways
test15 = runways.agg({'length_ft': ['min', 'max', 'mean', 'median']})
# print(test15)
# to visualize it like SQL, we need to transpose the data frame
# print(test15.T)

# select airport_ident, type, description, frequency_mhz
# from airport_freq
# join airports on airport_freq.airport_ref = airports.id
# where airports.ident = 'KLAX'
test16 = airport_freq.merge(airports[airports.ident == 'KLAX'][['id']],
                            left_on='airport_ref', right_on='id', how='inner')[
    ['airport_ident', 'type', 'description', 'frequency_mhz']]
# print(test16)

# select name, municipality from airports
# where ident = 'KLAX'
# union all
# select name, municipality from airports
# where ident = 'KLGB'
test17 = pd.concat([airports[airports.ident == 'KLAX'][['name', 'municipality']],
                    airports[airports.ident == 'KLGB'][['name', 'municipality']]])
# print(test17)
# to get a UNION, we'll need to add the following method
# print(test17.drop_duplicates())

# there is no INSERT in pandas, so we need to create a new dataframe
# containing the new data and the contact the two of them
# create table heroes (id integer, name text);
df1 = pd.DataFrame({'id': [1, 2], 'name': ['Harry Potter', 'Ron Weasley']})
# we create the new df with the element to "insert"
df2 = pd.DataFrame({'id': [3], 'name':  ['Hermione Granger']})
# we concat df1 and df2 to get final df
df_final = pd.concat([df1, df2]).reset_index(drop=True)
# print(df_final)

# update airports
# set home_link = 'http://www.lawa.org/welcomelax.aspx'
# where ident == 'KLAX'
airports.loc[airports['ident'] == 'KLAX', 'home_link'] = 'http://www.lawa.org/welcomelax.aspx'
# print(airports[airports.ident == 'KLAX'][['ident', 'home_link']])

# the best practice to delete something from a df is to create a new one
# with the elements we want to conserve
# delete from airport_freq
# where airport_ident = 'KLAX'
no_klax_freq1 = airport_freq[airport_freq.airport_ident != 'KLAX']
# another way by dropping the element directly
no_klax_freq2 = airport_freq.drop(airport_freq[airport_freq.airport_ident == 'KLAX'].index)
# print(no_klax_freq1[no_klax_freq1.airport_ident == 'KLAX'])
# print(no_klax_freq2[no_klax_freq2.airport_ident == 'KLAX'])

'''
We can export to a multitude of formats:

df.to_csv(...)  # csv file
df.to_hdf(...)  # HDF5 file
df.to_pickle(...)  # serialized object
df.to_sql(...)  # to SQL database
df.to_excel(...)  # to Excel sheet
df.to_json(...)  # to JSON string
df.to_html(...)  # render as HTML table
df.to_feather(...)  # binary feather-format
df.to_latex(...)  # tabular environment table
df.to_stata(...)  # Stata binary data files
df.to_msgpack(...)	# msgpack (serialize) object
df.to_gbq(...)  # to a Google BigQuery table.
df.to_string(...)  # console-friendly tabular output.
df.to_clipboard(...) # clipboard that can be pasted into Excel

'''

# Example of plotting...
top_10 = by_country.nlargest(10, columns='airport_count')
top_10.plot(
    x='iso_country',
    y='airport_count',
    kind='barh',
    figsize=(10, 7),
    title='Top 10 countries with most airports') 
plt.show()











