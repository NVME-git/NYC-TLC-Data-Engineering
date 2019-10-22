# NYC-TLC Data Engineering

New York City Taxi and Limousine Commission (TLC) Trip Record Data is loaded from AWS S3 into Redshift 
using Apache Airflow.

## Project scope

Trip and location data was obtained from a publicly available [S3 Bucket](https://s3.console.aws.amazon.com/s3/buckets/nyc-tlc/?region=eu-central-1) 
Data was gathered and moved to Redshift using a custom  Airflow Operator where it was transformed to an appropriate 
schema for analytics purposes. The main purpose of this exercise is to provide a mechanism for querying and sorting the
large volume of trip logs on a monthly basis by taxi zone and NYC borough. This data can then be used to redistribute 
vehicles appropriately where needed.

##### Taxi zone map of Brooklyn
![Brooklyn Map](maps/taxi_zone_map_brooklyn.jpg)

## Data investigation

The New York City Taxi and Limousine Commission [(NYC-TLC)](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page), 
created in 1971, is the agency responsible for licensing and 
regulating New York City's medallion (yellow) taxis,street hail livery (green) taxis, for-hire  vehicles  (FHVs), 
commuter vans,  and  para-transit  vehicles.The  TLC  collects trip  record information for each taxi and for-hire 
vehicle trip completed by our licensed drivers and vehicles. We  receive  taxi  trip  data  from the  technology service
providers (TSPs)  that provide  electronic metering  in  each  cab,  and  FHV trip data  from  the  app,  community 
livery,  black  car,  or  luxury limousine company, or base, who dispatched the trip.

In each trip record dataset, one row represents a single trip made by a TLC-licensed vehicle.
There are five major data sources that are used, dating from 2009 until present. 
See the [user guide](https://www1.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf) for this and more 
information.

#### Yellow taxi trips

Trips made by New York City’s iconic yellow taxis have been recorded and provided to the TLC since 2009. 
Yellow taxis are traditionally hailed by signaling to a driver who is on duty and seeking a passenger (street hail), 
but now they may also be hailed using an e-hail app like Curb or Arro. Yellow taxis are the only vehicles permitted to 
respond to a street hail from a passenger in all five boroughs. 
Records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, 
itemized fares, rate types, payment types, and driver-reported passenger counts. 
The records were  collected  and  provided  to  the  NYC  Taxi  and  Limousine  Commission  (TLC)  by technology service 
providers. The trip  data  was  not  created  by  the  TLC,  and  TLC cannot guarantee their accuracy.
The [yellow taxi data dictionary](https://data.cityofnewyork.us/api/views/biws-g3hs/files/eb3ccc47-317f-4b2a-8f49-5a684b0b1ecc?download=true&filename=data_dictionary_trip_records_yellow.pdf) 
can be downloaded for further reference.

####Green taxi trips

Green taxis, also known as boro taxis and street-hail liveries, were introduced in August of 2013 to improve   taxi   
service   and   availability   in   the boroughs. Green taxis may respond to street hails, but  only  in  the  areas  
indicated  in green  on  the map (i.e. above W 110 St/E 96thSt in Manhattan and in the boroughs).Records  include  
fields  capturing  pick-up  and drop-off    dates/times,    pick-up    and    drop-off locations,   trip   distances,   
itemized   fares,   rate types,    payment    types,    and    driver-reported passenger  counts.  As  with  the  yellow  
taxi  data, these  records  were  collected  and  provided  to the NYC Taxi and Limousine Commission (TLC) by technology 
service  providers. 
The [green taxi data dictionary](https://data.cityofnewyork.us/api/views/hvrh-b6nb/files/65544d38-ab44-4187-a789-5701b114a754?download=true&filename=data_dictionary_trip_records_green.pdf) 
can be downloaded for further reference.

#### For-hire vehicle (FHV) trips
 
FHV  data  includes  trip  data  from  high-volume  for-hire  vehicle  bases  (bases for  companies dispatching  10,000+
trip  per  day,  meaning  Uber,  Lyft,  Via,  and  Juno),  community  livery  bases, luxury limousine bases, and black 
car bases. The TLC began receiving FHV trip data from bases in 2015, but the amount of information that has been  
provided  has  changed  over  time.  In 2015,  only  the  dispatching  base  number,  pickup datetime, and the 
location of the pickup (see section on matching zone IDs below) were provided 
4to  the  TLC.  In  summer  of  2017,  the  TLC  mandated  that  the  companies  provide  the  drop-off date/time 
and the drop-off location. In 2017, the TLC also started to receive information on shared rides, like those offered 
in services like Lyft Line and Uber Pool. A trip is only considered shared if it was reserved specially with one of 
these services. See note below for more information on shared rides. After the high volume license type was created 
in Feb 2019, a high-volume license number was added. These are called FHVHV trips.
This is an overall identifier for app companies who may have multiple base licenses.  
Both the [FHV data dictionary](https://data.cityofnewyork.us/api/views/am94-epxh/files/0341cc01-520a-49eb-bc3c-94f6c35c6355?download=true&filename=data_dictionary_trip_records_fhv.pdf) 
and [FHVHV data dictionary](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf)
can be downloaded for further reference.

#### Shared trip insights

In the 2018 FHV records, there is a field called SR_Flag, which is supposed to indicate if the trip was a part of a 
shared ride chain offered by a High Volume FHScompany (e.g. Uber Pool, Lyft Line). For shared trips, the value is 1. 
For non-shared rides, this field is null. NOTE: For most High Volume FHScompanies, only shared rides that were 
requested AND matched to  another  shared-ride  request  over  the  course  of  the  journey  are  flagged.  
However,  Lyft (hvfhs_license_num=’HV0005’) also flags rides for which a shared ride was requested but another
 passenger was not successfully matched to share the trip—therefore, trips records with SR_Flag=1 from those two 
 bases could indicate EITHER a trip in ashared trip chain OR a trip for which a shared ride  was  requested  but  
 never  matched.  Users  should  anticipate  an  overcount  of  successfully shared trips completed by Lyft.
 Note also that Juno does not offer shared trips at this time.

#### Taxi zone lookup table

Each of the trip records contains a field corresponding to the location of the pickup or drop-off of the trip 
(or in FHV records before 2017, just the pickup), populated by numbers ranging from 1-263. 
These numbers correspond to taxi zones, which may be downloaded as a table or map/shapefile and matched to the trip 
records using a join. 
The data is currently available on the [Open Data Portal](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc)
, or on the  trip records    page on    the [TLC website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
, under Taxi Zone Maps and Lookup Tables.

##### Taxi zone map of Bronx
![Bronx Map](maps/taxi_zone_map_bronx.jpg)
 
 
