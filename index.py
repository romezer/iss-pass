import requests
import json
from datetime import datetime
import db

mydb = db.connect()
# db.createTable('orbital_data_rom_ezer')
mycursor = mydb.cursor()



api_endPoint = "http://api.open-notify.org/iss-pass.json"


with open('./cities_locations.json') as f:
  data = json.load(f)
temp_list = []
for key in data:
  value = data[key]
  PARAMS = {'lat': value['lat'], 'lon': value['lon'], 'n': 50}
  res = requests.get(url=api_endPoint, params=PARAMS)
  resJson = res.json()
  
  for i in resJson['response']:
    temp_list.append((key, datetime.fromtimestamp(i['risetime'])))

sql = "INSERT INTO orbital_data_rom_ezer (city, date) VALUES (%s, %s)"
mycursor.executemany(sql, temp_list)
mydb.commit()


# Clean the table before running the procedure
q2 = "DELETE FROM interview.city_stats_rom_ezer"
mycursor.execute(q2)
mydb.commit()

# call stored procedure 
mycursor.callproc('rom_ezer')
mydb.commit()



# Combine tabels
import csv
q = "SELECT a.city, a.avgPerDay, cities_union.population, cities_union.max_temperature, cities_union.min_temperature, cities_union.update_date  FROM interview.city_stats_rom_ezer as a JOIN (SELECT * FROM interview.city_stats_haifa UNION SELECT * FROM interview.city_stats_tel_aviv UNION SELECT * FROM interview.city_stats_eilat UNION SELECT * FROM interview.city_stats_beer_sheva) as cities_union ON a.city = cities_union.city"
mycursor.execute(q)
myResults = mycursor.fetchall()
fp = open('./file.csv', 'w')
myFile = csv.writer(fp)
myFile.writerows(myResults)
fp.close()



