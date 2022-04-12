import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist text,
    auth varchar,                --text
    firstName varchar,           --text
    gender varchar,
    itemInSession varchar,
    lastName varchar,            --text
    length varchar,              --float
    level varchar,               --text
    location varchar,            --text
    method varchar,
    page varchar,
    registration varchar,        --int
    sessionId varchar,           --int
    song varchar,                --text
    status varchar,
    ts varchar NOT NULL  SORTKEY  DISTKEY,
    userAgent varchar,
    userId varchar            --int, but there are actually nulls here
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id varchar NOT NULL SORTKEY DISTKEY,
    artist_latitude float,    
    artist_location text,
    artist_longitude float, 
    artist_name text,
    duration float,           
    num_songs int,       
    song_id varchar NOT NULL, 
    title text,
    year int             
)
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id int IDENTITY(0,1) SORTKEY,
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id varchar NOT NULL,
    location text,
    user_agent text
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id varchar NOT NULL SORTKEY,
    first_name text,
    last_name text,
    gender text,
    level text
)
""")

song_table_create = ("""
CREATE TABLE song (
    song_id varchar NOT NULL SORTKEY,
    title text,
    artist_id VARCHAR NOT NULL,
    year int,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE artist (
    artist_id varchar NOT NULL SORTKEY,
    name text,
    location text,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp NOT NULL SORTKEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    region '{}'
""").format(config.get('S3','LOG_DATA'), 
            config.get('IAM_ROLE', 'ARN'), 
            config.get('S3','LOG_JSONPATH'),
            config.get('AWS','REGION')
            )

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    region '{}'
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','SONG_JSONPATH'),
            config.get('AWS', 'REGION')
            )

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplay
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
TIMESTAMP 'epoch' + CAST(ts AS bigint)/1000 * interval '1 second' AS start_time,
userID          AS user_id,
level,

song_id,
artist_id,

sessionId               AS session_id,
location,
userAgent                AS user_agent
FROM staging_events e
JOIN staging_songs s
ON (s.duration=e.length 
    AND s.artist_name=e.artist 
    AND s.title=e.song)
WHERE user_ID IS NOT NULL
  AND page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
SELECT
    userId              AS user_id,
    firstName           AS first_name,
    lastName            AS last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO song
    (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist
    (artist_id, name, location, latitude, longitude)
SELECT
    artist_id,
    artist_name      AS name,
    artist_location  AS location,
    artist_latitude  AS latitude,
    artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time                         AS start_time,
    EXTRACT(hour FROM s.start_time)    AS hour,
    EXTRACT(day FROM s.start_time)     AS day,
    EXTRACT(week FROM s.start_time)    AS week,
    EXTRACT(month FROM s.start_time)   AS month,
    EXTRACT(year FROM s.start_time)    AS year,
    EXTRACT(weekday FROM s.start_time) AS weekday
FROM songplay s
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        songplay_table_create, user_table_create, song_table_create, 
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, 
                        artist_table_insert, time_table_insert]