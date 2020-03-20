# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events"
staging_songs_table_drop = "drop table if exists stg_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists stg_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession int,
        lastName varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId int,
        song varchar,
        status int,
        ts varchar,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    create table if not exists stg_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration double precision,
        year int
    )
""")

songplay_table_create = ("""
    create table if not exists songplays(
        songplay_id varchar primary key sortkey distkey, 
        user_id int references users(user_id), 
        song_id varchar references songs(song_id), 
        artist_id varchar references artists(artist_id), 
        start_time timestamp references time(start_time), 
        session_id int, 
        level varchar, 
        location varchar, 
        user_agent varchar
    )""")

user_table_create = ("""
    create table if not exists users(
        user_id int primary key sortkey, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    )""")

song_table_create = ("""
    create table if not exists songs(
        song_id varchar primary key sortkey, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration double precision
    )""")

artist_table_create = ("""
    create table if not exists artists(
        artist_id varchar primary key sortkey, 
        name varchar, 
        location varchar, 
        latitude double precision, 
        longitude double precision
    )""")


time_table_create = ("""
    create table if not exists time(
        start_time timestamp primary key sortkey, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    )""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
