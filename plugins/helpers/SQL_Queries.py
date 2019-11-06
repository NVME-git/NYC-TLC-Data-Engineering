class SqlQueries:
    create_taxi_zones='''
    CREATE TABLE IF NOT EXISTS public.taxi_zones (
        locationid VARCHAR(255),
        borough VARCHAR(255),
        zone VARCHAR(255),
        service_zone VARCHAR(255)
    )
    '''

    create_stage_green = '''
        CREATE TABLE IF NOT EXISTS public.stage_green (
            VendorID                VARCHAR,
            lpep_pickup_datetime    VARCHAR,
            lpep_dropoff_datetime   VARCHAR,
            store_and_fwd_flag      CHAR,
            RatecodeID              VARCHAR,
            PULocationID            VARCHAR,
            DOLocationID            VARCHAR,
            passenger_count         INT,
            trip_distance           DECIMAL,
            fare_amount             DECIMAL,
            extra                   DECIMAL,
            mta_tax                 DECIMAL,
            tip_amount              DECIMAL,    
            tolls_amount            DECIMAL,
            ehail_fee               DECIMAL,
            improvement_surcharge   DECIMAL,
            total_amount            DECIMAL,
            payment_type            VARCHAR,
            trip_type               VARCHAR,
            congestion_surcharge    DECIMAL
        )
    '''

    create_stage_yellow = '''
        CREATE TABLE IF NOT EXISTS public.stage_yellow (
            VendorID                VARCHAR,
            tpep_pickup_datetime    VARCHAR,
            tpep_dropoff_datetime   VARCHAR,
            passenger_count         INT,
            trip_distance           DECIMAL,
            RatecodeID              VARCHAR,
            store_and_fwd_flag      CHAR,
            PULocationID            VARCHAR,
            DOLocationID            VARCHAR,
            payment_type            VARCHAR,
            fare_amount             DECIMAL,
            extra                   DECIMAL,
            mta_tax                 DECIMAL,
            tip_amount              DECIMAL,
            tolls_amount            DECIMAL,
            improvement_surcharge   DECIMAL,
            total_amount            DECIMAL,
            congestion_surcharge    DECIMAL
        )
    '''

    create_stage_fhv = '''
        CREATE TABLE IF NOT EXISTS public.stage_fhv (
            dispatching_base_num    VARCHAR,
            pickup_datetime         VARCHAR,
            dropoff_datetime        VARCHAR,
            puLocationID            VARCHAR,
            doLocationID            VARCHAR,
            SR_Flag                 VARCHAR
        )
    '''

    create_stage_fhvhv = '''
            CREATE TABLE IF NOT EXISTS public.stage_fhvhv (
                hvfhs_license_num       VARCHAR,
                dispatching_base_num    VARCHAR,
                pickup_datetime         VARCHAR,
                dropoff_datetime        VARCHAR,
                puLocationID            VARCHAR,
                doLocationID            VARCHAR,
                SR_Flag                 VARCHAR
            )
        '''

    create_stage_tables = [
        create_taxi_zones,
        create_stage_green,
        create_stage_yellow,
        create_stage_fhv,
        create_stage_fhvhv
    ]

    edit_staging = """
        ALTER TABLE {table} rename column {columnA} to {columnB};
    """

    edit_stage_tables = [
        edit_staging.format(table='stage_green', columnA='lpep_pickup_datetime', columnB='pickup_datetime'),
        edit_staging.format(table='stage_green', columnA='lpep_dropoff_datetime', columnB='dropoff_datetime'),
        edit_staging.format(table='stage_yellow', columnA='tpep_pickup_datetime', columnB='pickup_datetime'),
        edit_staging.format(table='stage_yellow', columnA='tpep_dropoff_datetime', columnB='dropoff_datetime')
    ]

    create_time_table = """
        CREATE TABLE IF NOT EXISTS time(
            trip_timestamp  TIMESTAMP NOT NULL sortkey, 
            hour            INT, 
            day             INT, 
            week            INT, 
            month           INT, 
            year            INT, 
            weekday         INT
        );
    """

    create_taxi_table = """
        CREATE TABLE IF NOT EXISTS taxi_rides(
            VendorID                VARCHAR,
            pickup_datetime         VARCHAR,
            dropoff_datetime        VARCHAR,
            store_and_fwd_flag      CHAR,
            RatecodeID              VARCHAR,
            PULocationID            VARCHAR,
            DOLocationID            VARCHAR,
            passenger_count         INT,
            trip_distance           DECIMAL,
            fare_amount             DECIMAL,
            extra                   DECIMAL,
            mta_tax                 DECIMAL,
            tip_amount              DECIMAL,    
            tolls_amount            DECIMAL,
            improvement_surcharge   DECIMAL,
            total_amount            DECIMAL,
            payment_type            VARCHAR,
            congestion_surcharge    DECIMAL
        );
    """

    create_data_tables = [create_time_table, create_taxi_table]

    move_staging_time = '''
        INSERT INTO public.time (trip_timestamp, hour, day, week, month, year, weekday)
        SELECT 	to_date ({column}, 'YYYY-MM-DD HH24:MI:SS') as DT, 
                extract(hour from DT), 
                extract(day from DT), 
                extract(week from DT), 
                extract(month from DT), 
                extract(year from DT), 
                extract(dayofweek from DT)
        FROM public.{table}
    '''

    move_time_data = [
        move_staging_time.format(table='stage_green', column='pickup_datetime'),
        move_staging_time.format(table='stage_green', column='dropoff_datetime'),
        move_staging_time.format(table='stage_yellow', column='pickup_datetime'),
        move_staging_time.format(table='stage_yellow', column='dropoff_datetime')
    ]

    move_staging__taxi = '''
        ALTER TABLE taxi_rides APPEND FROM {table} 
        IGNOREEXTRA
    '''

    move_ride_data = [
        move_staging__taxi.format(table='stage_green'),
        move_staging__taxi.format(table='stage_yellow')
    ]

    analyse_location = '''
        select 
            zones.borough,
            sum(rides.total_amount) as total
        from 
            public.taxi_rides as rides
        join
            public.taxi_zones as zones
            on
                rides.{locationColumn} = zones.locationid
        group by
            zones.borough
        order by
            total DESC
        limit 
            10
        ;
    '''

    analysisQueries = [
        analyse_location.format(locationColumn='PULocationID'),
        analyse_location.format(locationColumn='DOLocationID')
    ]
