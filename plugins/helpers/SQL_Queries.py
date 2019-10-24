class SqlQueries:
    create_taxi_zones='''
    CREATE TABLE IF NOT EXISTS public.taxi_zones (
        borough VARCHAR(255),
        locationid VARCHAR(255),
        service_zone VARCHAR(255),
        zone VARCHAR(255)
    )
    '''

    create_stage_green = '''
        CREATE TABLE IF NOT EXISTS public.stage_green (
            VendorID                VARCHAR(255),
            lpep_pickup_datetime    VARCHAR(255),
            lpep_dropoff_datetime   VARCHAR(255),
            store_and_fwd_flag      VARCHAR(255),
            RatecodeID              VARCHAR(255),
            PULocationID            VARCHAR(255),
            DOLocationID            VARCHAR(255),
            passenger_count         VARCHAR(255),
            trip_distance           VARCHAR(255),
            fare_amount             VARCHAR(255),
            extra                   VARCHAR(255),
            mta_tax                 VARCHAR(255),
            tip_amount              VARCHAR(255),    
            tolls_amount            VARCHAR(255),
            ehail_fee               VARCHAR(255),
            improvement_surcharge   VARCHAR(255),
            total_amount            VARCHAR(255),
            payment_type            VARCHAR(255),
            trip_type               VARCHAR(255),
            congestion_surcharge    VARCHAR(255)
        )
    '''

    create_stage_yellow = '''
        CREATE TABLE IF NOT EXISTS public.stage_yellow (
            VendorID                VARCHAR(255),
            tpep_pickup_datetime    VARCHAR(255),
            tpep_dropoff_datetime   VARCHAR(255),
            passenger_count         VARCHAR(255),
            trip_distance           VARCHAR(255),
            RatecodeID              VARCHAR(255),
            store_and_fwd_flag      VARCHAR(255),
            PULocationID            VARCHAR(255),
            DOLocationID            VARCHAR(255),
            payment_type            VARCHAR(255),
            fare_amount             VARCHAR(255),
            extra                   VARCHAR(255),
            mta_tax                 VARCHAR(255),
            tip_amount              VARCHAR(255),
            tolls_amount            VARCHAR(255),
            improvement_surcharge   VARCHAR(255),
            total_amount            VARCHAR(255),
            congestion_surcharge    VARCHAR(255)
        )
    '''

    create_stage_fhv = '''
        CREATE TABLE IF NOT EXISTS public.stage_fhv (
            dispatching_base_num    VARCHAR(255),
            pickup_datetime         VARCHAR(255),
            dropoff_datetime        VARCHAR(255),
            puLocationID            VARCHAR(255),
            doLocationID            VARCHAR(255),
            SR_Flag                 VARCHAR(255)
        )
    '''

    create_stage_fhvhv = '''
            CREATE TABLE IF NOT EXISTS public.stage_fhvhv (
                hvfhs_license_num       VARCHAR(255),
                dispatching_base_num    VARCHAR(255),
                pickup_datetime         VARCHAR(255),
                dropoff_datetime        VARCHAR(255),
                puLocationID            VARCHAR(255),
                doLocationID            VARCHAR(255),
                SR_Flag                 VARCHAR(255)
            )
        '''

    create_stage_tables = [
        create_taxi_zones,
        create_stage_green,
        create_stage_yellow,
        create_stage_fhv,
        create_stage_fhvhv
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

    create_data_tables = [create_time_table]

    move_time_pickup_green = '''
        INSERT INTO public.time (trip_timestamp, hour, day, week, month, year, weekday)
        SELECT 	to_date (lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') as DT, 
                extract(hour from DT), 
                extract(day from DT), 
                extract(week from DT), 
                extract(month from DT), 
                extract(year from DT), 
                extract(dayofweek from DT)
        FROM public.stage_green
    '''

    move_time_drop_off_green = '''
            INSERT INTO public.time (trip_timestamp, hour, day, week, month, year, weekday)
            SELECT 	to_date (lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') as DT, 
                    extract(hour from DT), 
                    extract(day from DT), 
                    extract(week from DT), 
                    extract(month from DT), 
                    extract(year from DT), 
                    extract(dayofweek from DT)
            FROM public.stage_green
        '''

    move_time_pickup_yellow = '''
        INSERT INTO public.time (trip_timestamp, hour, day, week, month, year, weekday)
        SELECT 	to_date (tpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') as DT, 
                extract(hour from DT), 
                extract(day from DT), 
                extract(week from DT), 
                extract(month from DT), 
                extract(year from DT), 
                extract(dayofweek from DT)
        FROM public.stage_yellow
    '''

    move_time_drop_off_yellow = '''
            INSERT INTO public.time (trip_timestamp, hour, day, week, month, year, weekday)
            SELECT 	to_date (tpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') as DT, 
                    extract(hour from DT), 
                    extract(day from DT), 
                    extract(week from DT), 
                    extract(month from DT), 
                    extract(year from DT), 
                    extract(dayofweek from DT)
            FROM public.stage_yellow
        '''

    move_time_data = [
        move_time_pickup_green,
        move_time_drop_off_green,
        move_time_pickup_yellow,
        move_time_drop_off_yellow
    ]

    analyse_pick_up = '''
        select 
            zones.borough,
            sum(ride.total_amount) as total
        from 
            public.stage_green as ride
        join
            public.taxi_zones as zones
            on
                ride.pulocationid = zones.locationid
        group by
            zones.borough
        order by
            total DESC
        limit 
            10
        ;
    '''

    analyse_drop_off = '''
        select 
            zones.borough,
            sum(ride.total_amount) as total
        from 
            public.stage_green as ride
        join
            public.taxi_zones as zones
            on
                ride.dolocationid = zones.locationid
        group by
            zones.borough
        order by
            total DESC
        limit 
            10
        ;
    '''

    analysisQueries = [
        analyse_pick_up,
        analyse_drop_off
    ]
