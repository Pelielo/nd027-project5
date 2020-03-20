class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.userid, 
                songs.song_id, 
                songs.artist_id, 
                events.start_time, 
                events.sessionid, 
                events.level, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM stg_events
            WHERE page='NextSong') events
            LEFT JOIN stg_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM stg_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM stg_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM stg_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    ## Data Validation

    songplays_validation = ({
        'query': 'select count(*) from songplays where session_id is null', 
        'result': 0
    })

    songs_validation = ({
        'query': 'select count(*) from songs where title is null', 
        'result': 0
    })

    artists_validation = ({
        'query': 'select count(*) from artists where name is null', 
        'result': 0
    })

    users_validation = ({
        'query': 'select count(*) from users where first_name is null', 
        'result': 0
    })

    time_validation = ({
        'query': 'select count(*) from time where hour is null', 
        'result': 0
    })