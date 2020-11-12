# Objective: track my listening history on Spotify and store it in SQLite

import sqlalchemy
import pandas as pd 
import json
from datetime import datetime
import datetime
import requests
import sqlite3

DATABASE_LOC = "sqlite:///my_played_tracks.sqlite"
USER_ID = "stratogriz1"
SPOTITY_TOKEN = "----"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

# Extract

headers = {
	"Accept": "application/json",
	"Headers": "application/json",
	"Authorization": "Bearer {token}".format(token=SPOTITY_TOKEN)
}

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=2)
print(yesterday)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
print(yesterday_unix_timestamp)

r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

data = r.json()

song_names = []
artist_name = []
played_at = []
timestamps = []

for item in data["items"]:
	song_names.append(item["track"]["name"])
	artist_name.append(item["track"]["album"]["artists"][0]["name"])
	played_at.append(item["played_at"])
	timestamps.append(item["played_at"][:10])

song_dict = {
	"song_name": song_names,
	"artist_name": artist_name,
	"played_at": played_at,
	"timestamp": timestamps
}

df = pd.DataFrame(song_dict, columns = song_dict.keys())
print(df)

# Validate (Transform)
if check_if_valid_data(df):
    print("Data valid, proceed to Load stage")


# Load
engine = sqlalchemy.create_engine(DATABASE_LOC)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
	CREATE TABLE IF NOT EXISTS tr_played_tracks (
		song_name VARCHAR(200),
		artist_name VARCHAR(200),
		played_at VARCHAR(200),
		timestamp VARCHAR(200),
		CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
	)

"""

cursor.execute(sql_query)
print("Opened db successfully")

try:
	df.to_sql("tr_played_tracks", engine, index=False, if_exists='append')
except:
	print("Data already exists in the database")

conn.close()
print("Database closed successfully")