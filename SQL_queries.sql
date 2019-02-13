/* Query for trip by month & hour */

SELECT   Cast(Extract(month FROM pickup_datetime) AS STRING)     AS month, 
         Cast(Extract(date FROM pickup_datetime) AS STRING)      AS date, 
         Cast(Extract(dayofweek FROM pickup_datetime) AS STRING) AS dayofweek, 
         Cast(Extract(hour FROM pickup_datetime) AS STRING)      AS hour, 
         Count(*)                                                AS count 
FROM     `bigquery-PUBLIC-data.new_york.tlc_yellow_trips_2015` 
GROUP BY month, 
         date, 
         hour, 
         dayofweek 
ORDER BY month, 
         date, 
         hour, 
         dayofweek;

/* Query for Tip_location (heat density map for pickup area) */

SELECT   Cast(pickup_location_id AS INT64) AS id, 
         Count(*)                          AS count_pickup 
FROM     `bigquery-PUBLIC-data.new_york_taxi_trips.tlc_yellow_trips_2017` 
GROUP BY id;

/* Query for Avg Fare by passenger count */
SELECT passenger_count, 
       Avg(fare_amount) AS fare 
FROM   `bigquery-public-data.new_york.tlc_yellow_trips_2015` 
WHERE  passenger_count <> 0 
       AND passenger_count < 7 
GROUP  BY passenger_count ;

/* Query for Avg fare by week day */
SELECT   Avg(fare_amount)                                        AS fare, 
         Cast(Extract(dayofweek FROM pickup_datetime) AS STRING) AS dayofweek 
FROM     `bigquery-PUBLIC-data.new_york.tlc_yellow_trips_2015` 
WHERE    passenger_count <> 0 
AND      passenger_count < 7 
GROUP BY dayofweek;

/* Query for Tip data */
SELECT CASE 
         WHEN tippercentage < 0 THEN 'No Tip' 
         WHEN tippercentage BETWEEN 0 AND 5 THEN 'Less but still a Tip' 
         WHEN tippercentage BETWEEN 5 AND 10 THEN 'Decent Tip' 
         WHEN tippercentage > 10 THEN 'Good Tip' 
         ELSE 'Something different' 
       end AS TipRange, 
       hr, 
       wk, 
       tripmonth, 
       trips, 
       tips, 
       averagespeed, 
       averagedistance, 
       tippercentage, 
       tipbin 
FROM   (SELECT Extract(hour FROM pickup_datetime) 
               AS 
                      Hr, 
               Extract(dayofweek FROM pickup_datetime) 
               AS 
                      Wk, 
               Extract (month FROM pickup_datetime) 
               AS 
                      TripMonth, 
               CASE 
                 WHEN tip_amount = 0 THEN 'No Tip' 
                 WHEN ( tip_amount > 0 
                        AND tip_amount <= 5 ) THEN '0-5' 
                 WHEN ( tip_amount > 5 
                        AND tip_amount <= 10 ) THEN '5-10' 
                 WHEN ( tip_amount > 10 
                        AND tip_amount <= 20 ) THEN '10-20' 
                 WHEN tip_amount > 20 THEN '> 20' 
                 ELSE 'other' 
               end 
               AS 
                      Tipbin, 
               Count(*) 
                      Trips, 
               Sum(tip_amount) 
               AS 
                      Tips, 
               Round(Avg(trip_distance / Timestamp_diff(dropoff_datetime, 
                                         pickup_datetime, 
                                         second)) * 
                     3600, 1) 
               AS 
                      AverageSpeed, 
               Round(Avg(trip_distance), 1) 
               AS 
                      AverageDistance, 
               Round(Avg(( tip_amount ) / ( total_amount - tip_amount )) * 100, 
               3) AS 
               TipPercentage 
        FROM   `bigquery-public-data.new_york.tlc_yellow_trips_2015` 
        WHERE  trip_distance > 0 
               AND fare_amount / trip_distance BETWEEN 2 AND 10 
               AND dropoff_datetime > pickup_datetime 
        GROUP  BY 1, 
                  2, 
                  3, 
                  tip_amount, 
                  total_amount, 
                  tipbin) ;

/* Query For Average speed */
SELECT Extract(hour FROM pickup_datetime) hour, 
       Round(Avg(trip_distance / Timestamp_diff(dropoff_datetime, 
                                 pickup_datetime, 
                                 second)) * 
             3600, 1)                     speed 
FROM   `bigquery-public-data.new_york.tlc_yellow_trips_2015` 
WHERE  trip_distance > 0 
       AND fare_amount / trip_distance BETWEEN 2 AND 10 
       AND dropoff_datetime > pickup_datetime 
GROUP  BY hour 
ORDER  BY hour ;
