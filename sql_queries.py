import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS  staging_events"
staging_songs_table_drop = "DROP TABLE  IF EXISTS  staging_songs"
songplay_table_drop = "DROP TABLE  IF EXISTS  songplays"
user_table_drop = "DROP TABLE  IF EXISTS  users"
song_table_drop = "DROP TABLE  IF EXISTS  songs"
artist_table_drop = "DROP TABLE  IF EXISTS  artists"
time_table_drop = "DROP TABLE  IF EXISTS  time"

# CREATE TABLES

# Create staging_events Table
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT
    );
""")

# Create staging_songs Table
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs           INTEGER,
        artist_id           TEXT,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            FLOAT,
        year                SMALLINT
    );
""")

# Create the songplays table with a specific distribution style and sort key.
# DISTSTYLE KEY specifies that the rows are distributed across nodes based on the distribution key (user_id).
# SORTKEY (start_time) For improving query performance by sorting the data on the start_time column.
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL SORTKEY,
        user_id        TEXT NOT NULL DISTKEY,
        level          TEXT,
        song_id        TEXT,
        artist_id      TEXT,
        session_id     INTEGER,
        location       TEXT,
        user_agent     TEXT
    ) DISTSTYLE KEY;
""")

# Create the users table with DISTSTYLE ALL, This mean that the entire table is replicated on each node.
# SORTKEY (user_id) used for fast retrieval of data based on user_id.
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     TEXT PRIMARY KEY SORTKEY,
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT
    ) diststyle all;
""")

# Create the songs table with a specific distribution style.
# DISTSTYLE KEY uses artist_id as the distribution key to distribute rows across nodes.
# SORTKEY (song_id) sorts the data based on song_id for efficient retrieval.
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     TEXT PRIMARY KEY SORTKEY,
        title       TEXT,
        artist_id   TEXT DISTKEY,
        year        SMALLINT,
        duration    FLOAT
    ) DISTSTYLE KEY;
""")

# Create the artists table with DISTSTYLE ALL, replicating the entire table on each node.
# SORTKEY (artist_id) sorts the data based on artist_id for efficient retrieval.
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   TEXT PRIMARY KEY SORTKEY,
        name        TEXT,
        location    TEXT,
        latitude    FLOAT,
        longitude   FLOAT
    ) diststyle all;
""")


# Create the time table with a distribution style based on the year column.
# DISTSTYLE KEY designates year as the distribution key.
# SORTKEY (start_time) For improving query performance by sorting the data on the start_time column.
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT DISTKEY,
        weekday     SMALLINT
    ) DISTSTYLE KEY;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format(
    'staging_songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# FINAL TABLES


# This query inserts data into the songplays table. It combines data from staging_events and staging_songs.
# The TIMESTAMP conversion is used to convert epoch time (in milliseconds) to a regular timestamp.
# The LEFT JOIN ensures that every event is included even if there is no matching song.
# The WHERE clause filters for rows where the 'page' column is 'NextSong', indicating actual song plays.
songplay_table_insert = ("""
     INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON
        e.song = s.title AND
        e.artist = s.artist_name AND
        ABS(e.length - s.duration) < 2
    WHERE
        e.page = 'NextSong'
""")


# This query populates the users table with distinct users from the staging_events table.
# SELECT DISTINCT ensures that each user is inserted only once.
user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
""")


# Inserts distinct songs from the staging_songs table into the songs table.
# The DISTINCT clause is used to prevent duplicate song entries.
song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")


# Populates the artists table with distinct artists from the staging_songs table.
# The DISTINCT clause is used to prevent duplicate artist entries.
artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

# Inserts distinct time data into  time table.
# The WITH clause creates a temporary table (temp_time)  that converts the epoch time to a timestamp.
# The final SELECT extracts various time components from  this timestamp.
time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy ]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
