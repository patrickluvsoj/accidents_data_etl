
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
accident_staging_table_drop = "DROP TABLE IF EXISTS accident_staging_table"
city_staging_table_drop = "DROP TABLE IF EXISTS city_staging_table"
covid_staging_table_drop = "DROP TABLE IF EXISTS covid_staging_table"
accident_table_drop = "DROP TABLE IF EXISTS accident_table CASCADE"
city_table_drop = "DROP TABLE IF EXISTS city_table CASCADE"
covid_table_drop = "DROP TABLE IF EXISTS covid_table CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time_table CASCADE"

# CREATE STAGING TABLES 
create_accident_staging_table = ("""
    CREATE TABLE IF NOT EXISTS accident_staging_table (
        id                      VARCHAR,
        source                  VARCHAR,
        tmc                     REAL,
        severity                INT,
        start_time              TIMESTAMP,
        end_time                TIMESTAMP,
        start_lat               NUMERIC(9,6),
        start_lng               NUMERIC(9,6),
        end_lat                 NUMERIC(9,6),
        end_lng                 NUMERIC(9,6),
        distance                REAL,
        description             VARCHAR(MAX),
        number                  REAL,
        street                  VARCHAR,
        side                    VARCHAR(1),
        city                    VARCHAR,
        county                  VARCHAR,
        state                   VARCHAR(5),
        zipcode                 VARCHAR,
        country                 VARCHAR,
        timezone                VARCHAR,
        airport_code            VARCHAR,
        weather_time            TIMESTAMP,
        temprature              REAL,
        wind_chill              REAL,
        humidity                REAL,
        pressure                REAL,
        visibility              REAL,
        wind_direction          VARCHAR,
        wind_speed              REAL,
        percipiration           REAL,
        weather_condition       VARCHAR,
        amenity                 BOOLEAN,          
        bump                    BOOLEAN,
        crossing                BOOLEAN,
        give_way                BOOLEAN,
        junction                BOOLEAN,
        no_exit                 BOOLEAN,
        railway                 BOOLEAN,
        roundabout              BOOLEAN,
        station                 BOOLEAN,
        stop                    BOOLEAN,
        traffic_calming         BOOLEAN,
        traffic_signal          BOOLEAN,
        turning_loop            BOOLEAN,
        sunrise_sunset          VARCHAR,
        civil_twilight          VARCHAR,
        nautical_twilight       VARCHAR,
        astronomical_twilight   VARCHAR
    )
""")

create_city_staging_table = ("""
    CREATE TABLE IF NOT EXISTS city_staging_table (
        count                  INT,
        city                   VARCHAR,
        number_of_veterans     INT,
        male_population        INT,
        foreign_born           INT,
        average_household_size REAL,
        median_age             REAL,
        state                  VARCHAR,
        race                   VARCHAR,
        total_population       INT,
        state_code             VARCHAR,
        female_population      INT
    )
""")

create_covid_staging_table = ("""
    CREATE TABLE IF NOT EXISTS covid_staging_table (
        country_name        VARCHAR,
        day                 TIMESTAMP,
        stringency_index    REAL,
        total_vaccinations  REAL,
        total_deaths        REAL,
        total_cases         REAL,
        daily_cases         REAL,
        biweekly_cases      REAL
    )
""")

# CREATE FACT/DIMENSION TABLES
# Must add primary key referencesxs
create_accident_table = ("""
    CREATE TABLE IF NOT EXISTS accident_table (
        accident_id     VARCHAR PRIMARY KEY,
        severity        INT,
        start_time_key  TIMESTAMP REFERENCES time_table (start_time_key) NOT NULL,
        end_time        TIMESTAMP,
        date_key        TIMESTAMP REFERENCES covid_table (date_key) NOT NULL, 
        description     VARCHAR(MAX), 
        city_key        VARCHAR REFERENCES city_table (city_key) NOT NULL DISTKEY,
        temprature      REAL,
        wind_chill      REAL,
        humidity        REAL,
        pressure        REAL,
        visibility      REAL,
        wind_speed      REAL,
        weather_condition  VARCHAR,
        sunrise_sunset  VARCHAR
    )
""")

create_time_table = ("""
    CREATE TABLE IF NOT EXISTS time_table(
        start_time_key TIMESTAMP PRIMARY KEY,
        start_time     TIMESTAMP SORTKEY,
        hour           INT,
        day            INT,
        week           INT,
        month          INT,
        year           INT,
        weekend        BOOLEAN 
    )DISTSTYLE ALL
""")

create_covid_table = ("""
    CREATE TABLE IF NOT EXISTS covid_table(
        date_key             TIMESTAMP PRIMARY KEY,
        total_cases          INT SORTKEY,
        total_deaths         INT,
        daily_cases          INT,
        biweekly_cases       INT,
        total_vaccinations   INT        
    )DISTSTYLE ALL
""")

create_city_table = ("""
    CREATE TABLE IF NOT EXISTS city_table(
        city_key                 VARCHAR PRIMARY KEY,
        total_population         INT SORTKEY,
        average_household_size   REAL,
        median_age               INT,
        male_population          INT,     
        female_population        INT,
        state                    VARCHAR
    )DISTSTYLE ALL
""")


# STAGING TABLES
staging_accident_copy = ("""
    COPY accident_staging_table FROM {}
    iam_role {}
    CSV
    IGNOREHEADER 1
    REGION 'us-west-2'
""").format(config['S3']['ACCIDENT_DATA'], 
            config['IAM_ROLE']['ARN'])

staging_city_copy = ("""
    COPY city_staging_table FROM {}
    iam_role {}
    CSV
    IGNOREHEADER 1
    REGION 'us-west-2'
""").format(config['S3']['CITY_DATA'], 
            config['IAM_ROLE']['ARN'])

staging_covid_copy = ("""
    COPY covid_staging_table FROM {}
    iam_role {}
    CSV
    IGNOREHEADER 1
    REGION 'us-west-2'
""").format(config['S3']['COVID_DATA'], 
            config['IAM_ROLE']['ARN'])


# FINAL TABLES
accident_table_insert = ("""
    INSERT INTO accident_table (accident_id, severity, start_time_key, end_time, date_key, 
                                description, city_key, temprature, wind_chill, humidity, 
                                pressure, visibility, wind_speed, weather_condition, sunrise_sunset)
    SELECT id, severity, start_time, end_time, start_time::date as date, 
           description, city, temprature, wind_chill, humidity, pressure, visibility, wind_speed, 
           weather_condition, sunrise_sunset
    FROM accident_staging_table
""")

city_table_insert = ("""
    INSERT INTO city_table (city_key, total_population, average_household_size, median_age,
                            male_population, female_population, state)
    SELECT DISTINCT city, total_population, average_household_size, median_age, 
                    male_population, female_population, state
    FROM city_staging_table
""")

covid_table_insert = ("""
    INSERT INTO covid_table (date_key, total_cases, total_deaths, daily_cases,
                             biweekly_cases, total_vaccinations)
    SELECT DISTINCT day, total_cases, total_deaths, daily_cases,
                    biweekly_cases, total_vaccinations 
    FROM covid_staging_table
    WHERE country_name = 'United States'
""")

time_table_insert = ("""
    INSERT INTO time_table (start_time_key, start_time, hour, day,
                            week, month, year, weekend)
    SELECT DISTINCT a.start_time,
           a.start_time,
           EXTRACT(hour FROM a.start_time),
           EXTRACT(day FROM a.start_time),
           EXTRACT(week FROM a.start_time),
           EXTRACT(month FROM a.start_time),
           EXTRACT(year FROM a.start_time),
           CASE
               WHEN 5 = EXTRACT(weekday FROM a.start_time) THEN True
               WHEN 6 = EXTRACT(weekday FROM a.start_time) THEN True
               ELSE False
           END
    FROM (SELECT start_time 
    FROM accident_staging_table) a;
""")

# QUERY LISTS
create_table_queries = [create_accident_staging_table, create_city_staging_table, create_covid_staging_table,
                        create_time_table, create_covid_table, create_city_table, create_accident_table]
drop_table_queries = [accident_staging_table_drop, city_staging_table_drop, covid_staging_table_drop, 
                      covid_table_drop, city_table_drop, time_table_drop, accident_table_drop]

copy_table_queries = [staging_covid_copy, staging_city_copy, staging_accident_copy]
insert_table_queries = [accident_table_insert, city_table_insert, covid_table_insert, time_table_insert]
