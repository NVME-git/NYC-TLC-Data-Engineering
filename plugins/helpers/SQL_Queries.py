class SqlQueries:
    create_taxi_zones='''
    CREATE TABLE IF NOT EXISTS public.taxi_zones (
        borough VARCHAR(255),
        locationid VARCHAR(255),
        service_zone VARCHAR(255),
        zone VARCHAR(255)
    )
    '''
    old_create_stage_green = '''
    CREATE TABLE IF NOT EXISTS public.stage_green (
        VendorID                VARCHAR(255),
        lpep_pickup_datetime    VARCHAR(255),
        lpep_dropoff_datetime   VARCHAR(255),
        Store_and_fwd_flag      VARCHAR(255),
        RateCodeID              VARCHAR(255),
        Pickup_longitude        VARCHAR(255),
        Pickup_latitude	        VARCHAR(255),
        Dropoff_longitude	    VARCHAR(255),
        Dropoff_latitude        VARCHAR(255),
        Passenger_count	        VARCHAR(255),
        Trip_distance	        VARCHAR(255),
        Fare_amount	            VARCHAR(255),
        Extra	                VARCHAR(255),
        MTA_tax	                VARCHAR(255),
        Tip_amount	            VARCHAR(255),
        Tolls_amount	        VARCHAR(255),
        Ehail_fee	            VARCHAR(255),
        Total_amount	        VARCHAR(255),
        Payment_type	        VARCHAR(255),
        Trip_type               VARCHAR(255)
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

    create_tables = [create_taxi_zones, create_stage_green, create_stage_yellow, create_stage_fhv, create_stage_fhvhv]

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
