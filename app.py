from config import *
import pandas as pd 
import psycopg2 
from timeloop import Timeloop
import datetime as dt 
from datetime import timedelta, datetime
import pytz
import json
import geopandas as gpd
import requests
from dateutil import tz

def db_connect():
	# conn = psycopg2.connect(host=host, options='-c statement_timeout=30s', dbname=database, user=user, password=password)    	
	conn = psycopg2.connect(host="localhost", options='-c statement_timeout=30s', dbname="testing", user="postgres", password="9664241907")    
	cursor = conn.cursor()
	return conn, cursor 

def create_db():
	print("Create Databases")
	conn, cursor = db_connect()
	sql = "CREATE TABLE IF NOT EXISTS weather_coords (id SERIAL NOT NULL, \
		lon float, lat float, \
		CONSTRAINT weather_coords_pkey PRIMARY KEY (id));"
	cursor.execute(sql,)
	conn.commit()    
	sql = "CREATE TABLE IF NOT EXISTS weather_dates (id SERIAL NOT NULL, \
		datetime timestamp, \
		CONSTRAINT weather_dates_pkey PRIMARY KEY (id));"
	cursor.execute(sql,)
	conn.commit()    
	sql = "CREATE TABLE IF NOT EXISTS weather_values (id SERIAL NOT NULL, \
		id_weather_coordsfk integer, id_weather_datesfk integer ,\
		temp float, feels_like float, temp_min float, temp_max float, pressure integer, humidity integer,\
		visibility integer, wind_speed float, wind_deg float, clouds_all integer, description text, icon text, \
		CONSTRAINT weather_values_pkey PRIMARY KEY (id),\
		FOREIGN KEY (id_weather_coordsfk) REFERENCES weather_coords (id),\
		FOREIGN KEY (id_weather_datesfk) REFERENCES weather_dates (id));"
	cursor.execute(sql,)
	conn.commit()
	conn.close()

def readfiles_data(name):
	with open(name,'r') as f:
		file_data = json.load(f)
	f.close()
	return file_data

def add_data(id_coords, id_date, main, wind, desc, data,cursor,conn):
	sql = 'insert into weather_values(id_weather_coordsfk,id_weather_datesfk,temp,\
				feels_like,temp_min,temp_max,pressure,humidity,visibility,wind_speed,wind_deg,\
				clouds_all,description,icon) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'				
	cursor.execute(sql, (id_coords[0], id_date[0], main['temp'], main['feels_like'], 
						 main['temp_min'], main['temp_max'],main['pressure'],main['humidity'],
						 data['visibility'],wind['speed'],wind['deg'],
						 data['clouds']['all'],desc['description'],desc['icon'],))
	conn.commit()

tl = Timeloop()
@tl.job(interval=timedelta(seconds=60))
def sample_job_every_1000s():
	nicosia_areas = 'nicosia_mun_areas.json'
	postal_codes = 'nicosia_postcodes_with_population.json'
	data2 = readfiles_data(nicosia_areas)
	data1 = readfiles_data(postal_codes)
	gdf = gpd.GeoDataFrame.from_features(data1["features"])
	x = gdf['geometry'].centroid.x
	y = gdf['geometry'].centroid.y
	gdf1 = gpd.GeoDataFrame.from_features(data2["features"])            
	x1 = gdf1['geometry'].centroid.x
	y1 = gdf1['geometry'].centroid.y
	fx = pd.concat([x,x1]).reset_index(drop=True)
	fy = pd.concat([y,y1]).reset_index(drop=True)

	for k in fx.index:
		conn, cursor = db_connect()
		url = 'https://api.openweathermap.org/data/2.5/weather?lat='+str(fx[k])+'&lon='+str(fy[k])+'&appid=61294daffa9aebd52ab68eec334b5882&units=metric'
		try:
			request = requests.get(url)
		except (
		requests.ConnectionError,
		requests.exceptions.ReadTimeout,
		requests.exceptions.Timeout,
		requests.exceptions.ConnectTimeout,) as e:        	
			print(e)
		if request: 
			data = request.json()

			data['time']=datetime.fromtimestamp(data['dt'], pytz.timezone("Asia/Nicosia")).strftime("%Y-%m-%d %H:%M")
			try:
				lat = data['coord']['lat']
				lon = data['coord']['lon']   
				time = data['time']
				sql = 'select id from weather_coords where lat = %s and lon = %s'
				cursor.execute(sql,(lat,lon,))    
				id_coords = cursor.fetchone()
				conn.commit()
				if not id_coords:
					sql = 'insert into weather_coords (lat,lon) values (%s, %s) returning id'
					cursor.execute(sql,(lat,lon))
					id_coords = cursor.fetchone()
					conn.commit()
				sql = 'select id from weather_dates where datetime = %s'
				cursor.execute(sql,(time,))
				id_date = cursor.fetchone()
				conn.commit()
				main = data['main']
				wind = data['wind']
				desc = data['weather'][0]
				if not id_date:
					sql = 'insert into weather_dates (datetime) values (%s) returning id'
					cursor.execute(sql,(time,))
					id_date = cursor.fetchone()
					conn.commit()
					add_data(id_coords, id_date, main, wind, desc, data, cursor, conn)				
				else:
					sql = 'select * from weather_values where id_weather_coordsfk=%s and id_weather_datesfk=%s and temp=%s and \
					feels_like=%s and temp_min=%s and temp_max=%s and pressure=%s and humidity=%s and visibility=%s and wind_speed=%s and wind_deg=%s and \
					clouds_all=%s and description=%s and icon=%s'
					cursor.execute(sql, (id_coords[0], id_date[0], main['temp'], main['feels_like'], 
										 main['temp_min'], main['temp_max'],main['pressure'],main['humidity'],
										 data['visibility'],wind['speed'],wind['deg'],
										 data['clouds']['all'],desc['description'],desc['icon'],))
					data_db = cursor.fetchall()
					if not data_db:
						add_data(id_coords, id_date, main, wind, desc, data,cursor,conn)											
			except psycopg2.Error as e:
				print(e)
				continue
			finally:
				conn.close()

if __name__ == "__main__":
	create_db()
	tl.start(block=True)
