USE FinalProject;
CREATE VIEW YDataSpeedTimeJun AS
SELECT  RIGHT(sec_to_time((TIME_TO_SEC(y1.tpep_dropoff_datetime) - TIME_TO_SEC(y1.tpep_pickup_datetime))),5)  AS Trip_time_in_minutes,
		(y1.Trip_distance / ((TIME_TO_SEC(y1.tpep_dropoff_datetime) - TIME_TO_SEC(y1.tpep_pickup_datetime)) / 3600) ) AS Trip_Speed,
        y1.Passenger_count, y1.Trip_distance, ST_Distance_Sphere( 
 point(pickup_longitude, pickup_latitude),
 point(dropoff_longitutde, dropoff_latitude)
 ) * .000621371192 AS P2PDistance,
        y1.fare_amount, y1.total_amount, y1.tpep_pickup_datetime, y1.tpep_dropoff_datetime,
        y1.pickup_longitude, y1.pickup_latitude, y1.dropoff_Longitutde, y1.dropoff_latitude
FROM YellowTripJun2015 y1
WHERE y1.Passenger_count < 3;


CREATE VIEW YellowTaxiViewJun AS
SELECT A.Trip_time_in_minutes ,B.Trip_time_in_minutes AS Second_trip_time, A.Trip_distance, B.Trip_distance AS Second_trip_distance, A.Trip_speed, B.Trip_speed AS Second_speed ,
	   A.Passenger_count, B.Passenger_count AS Second_trip_passenger, A.tpep_pickup_datetime, B.tpep_pickup_datetime AS Second_pickup, A.tpep_dropoff_datetime, B.tpep_dropoff_datetime AS Second_dropoff,
       (A.Trip_speed + B.Trip_speed)/2 AS Average_speed, 
(ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(B.pickup_longitude, B.pickup_latitude)
 ) * .000621371192) / ((A.Trip_speed + B.Trip_speed)/2) * 60 AS Time_o1_to_o2,
 ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(B.dropoff_longitutde, B.dropoff_latitude)
 ) * .000621371192 AS o1_to_d2Dist,
 ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(A.dropoff_longitutde, A.dropoff_latitude)
 ) * .000621371192 AS o1_to_d1Dist,
 ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(B.pickup_longitude, B.pickup_latitude)
 ) * .000621371192 AS o1_to_o2Dist,
 ST_Distance_Sphere( 
 point(B.pickup_longitude, B.pickup_latitude),
 point(A.pickup_longitude, A.pickup_latitude)
 ) * .000621371192 AS o2_to_o1Dist,
 ST_Distance_Sphere( 
 point(B.pickup_longitude, B.pickup_latitude),
 point(A.dropoff_longitutde, A.dropoff_latitude)
 ) * .000621371192 AS o2_to_d1Dist,
 ST_Distance_Sphere( 
 point(B.pickup_longitude, B.pickup_latitude),
 point(B.dropoff_longitutde, B.dropoff_latitude)
 ) * .000621371192 AS o2_to_d2Dist,
 ST_Distance_Sphere( 
 point(A.dropoff_longitutde, A.dropoff_latitude),
 point(B.dropoff_longitutde, B.dropoff_latitude)
 ) * .000621371192 AS d1_to_d2Dist,
 ST_Distance_Sphere( 
 point(B.dropoff_longitutde, B.dropoff_latitude),
 point(A.dropoff_longitutde, A.dropoff_latitude)
 ) * .000621371192 AS d2_to_d1Dist
FROM ydataspeedtimejun AS B
	CROSS JOIN
    ydataspeedtimejun AS A
WHERE B.Passenger_count + A.Passenger_count < 4;


CREATE VIEW YELLOWallImportantDataJun AS
SELECT *, (o1_to_d2Dist / Average_speed) * 60 AS Time_o1_to_d2,
	 (o1_to_d1Dist / Average_speed) * 60 AS Time_o1_to_d1,
     (o2_to_o1Dist / Average_speed) * 60 AS Time_o2_to_o1,
     (o2_to_d1Dist / Average_speed) * 60 AS Time_o2_to_d1,
     (o2_to_d2Dist / Average_speed) * 60 AS Time_o2_to_d2,
     (d1_to_d2Dist / Average_speed) * 60 AS Time_d1_to_d2,
     (d2_to_d1Dist / Average_speed) * 60 AS Time_d2_to_d1,
     (o1_to_o2Dist+o2_to_d1Dist+d1_to_d2Dist) AS Sequence1Dist,
     (o1_to_o2Dist+ o2_to_d2Dist + d2_to_d1Dist) AS Sequence2Dist,
     (o2_to_o1Dist + o1_to_d1Dist + d1_to_d2Dist) AS Sequence3Dist,
     (o2_to_o1Dist + o1_to_d2Dist + d2_to_d1Dist) AS Sequence4Dist,
     (o1_to_d1Dist + o2_to_d2Dist) AS totalDistanceP2P
FROM yellowtaxiviewjun;


-- For 1st sequence
USE FinalProject;
CREATE VIEW FinalYellowTaxiViewJun1 AS
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P, 
	((Time_o2_to_o1 + Time_o2_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) AS comparedTime1
FROM YELLOWallImportantDataJun
WHERE Second_pickup > tpep_pickup_datetime 
	AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist OR totalDistanceP2P > Sequence4Dist)
	AND ((Time_o2_to_o1 + Time_o2_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) <= 5;



-- For 2nd Sequence
USE FinalProject;
CREATE VIEW FinalYellowTaxiViewJun2 AS
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P,
	((Time_o2_to_o1 + Time_o2_to_d2 + Time_d1_to_d2) - Time_o1_to_d1) AS comparedTime2
FROM YELLOWallImportantDataJun
WHERE Second_pickup > tpep_pickup_datetime 
	AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist OR totalDistanceP2P > Sequence4Dist)
	AND ((Time_o2_to_o1 + Time_o2_to_d2 + Time_d1_to_d2) - Time_o1_to_d1) <= 5;


-- For 3rd Sequence
USE FinalProject;
CREATE VIEW FinalYellowTaxiViewJun3 AS
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P,
	((Time_o2_to_o1 + Time_o1_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) AS comparedTime3
FROM YELLOWallImportantDataJun
WHERE Second_pickup < tpep_pickup_datetime 
	AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist OR totalDistanceP2P > Sequence4Dist)
	AND ((Time_o2_to_o1 + Time_o1_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) <= 5;


-- For 4th Sequence
USE FinalProject;
CREATE VIEW FinalYellowTaxiViewJun4 AS
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P, 
	((Time_o2_to_o1 + Time_o1_to_d2 + Time_d2_to_d1) - Time_o1_to_d1) AS comparedTime4
FROM YELLOWallImportantDataJun
WHERE Second_pickup < tpep_pickup_datetime 
	AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist OR totalDistanceP2P > Sequence4Dist)
	AND ((Time_o2_to_o1 + Time_o1_to_d2 + Time_d2_to_d1) - Time_o1_to_d1) <= 5;
