


/*
to copy the structure of an existing table
SELECT sql FROM sqlite_master WHERE type='table' AND name='mytable'
*/



/*
Following lines return the service_id that run on the chosen day
*/


DROP TABLE IF EXISTS running_services;

CREATE TABLE running_services(
	trip_id
)
;

INSERT INTO running_services
SELECT trip_id
FROM trips
WHERE service_id 
IN
	(SELECT service_id
	FROM calendar 
	WHERE start_date <= (SELECT day from parameters)
	AND end_date >= (SELECT day from parameters)
	AND 
		(CASE WHEN strftime('%w',(SELECT day from parameters)) = '0' THEN sunday 
			  WHEN strftime('%w',(SELECT day from parameters)) = '1' THEN monday 
			  WHEN strftime('%w',(SELECT day from parameters)) = '2' THEN tuesday 
			  WHEN strftime('%w',(SELECT day from parameters)) = '3' THEN wednesday 
			  WHEN strftime('%w',(SELECT day from parameters)) = '4' THEN thursday
			  WHEN strftime('%w',(SELECT day from parameters)) = '5' THEN friday 
			  WHEN strftime('%w',(SELECT day from parameters)) = '6' THEN saturday
		END) = 1
	UNION
		SELECT service_id
		FROM calendar_dates 
		WHERE date = (SELECT day from parameters)
		AND exception_type = 1
	EXCEPT 
		SELECT service_id
		FROM calendar_dates 
		WHERE date = (SELECT day from parameters)
		AND exception_type = 2
	) 
;



/*
Work on table direct, containing the direct od between ori and dest
*/


DROP TABLE IF EXISTS DIRECT;


CREATE TABLE DIRECT(
    trip_id, 
    stop_id1, 
    stop_id2, 
    departure_time, 
    arrival_time, 
    trip_duration
    )
;

INSERT INTO DIRECT
    SELECT 
        st1.trip_id,
        st1.stop_id, 
        st2.stop_id, 
        st1.departure_time, 
        st2.arrival_time, 
        (strftime('%s',st2.arrival_time)-strftime('%s',st1.departure_time))/60
    FROM stop_times AS st1
    INNER JOIN stop_times AS st2
    ON st1.trip_id = st2.trip_id 
    AND st1.stop_sequence < st2.stop_sequence
    WHERE st1.trip_id IN
    	(SELECT trip_id FROM running_services)
;



/*
Work on table ONE_TRANSFER, containing the od with one transfer between ori and dest
*/

DROP TABLE IF EXISTS ONE_TRANSFER;

CREATE TABLE ONE_TRANSFER(
	trip1,
	trip2,
	origin,
	transfer,
	destination,
	departure,
	transfer_arrival,
	transfer_departure,
	arrival,
	transfer_duration,
	one_transfer_duration
);

INSERT INTO ONE_TRANSFER
SELECT 
	st1.trip_id AS trip1,
	st2.trip_id AS trip2,
	st1.stop_id1 AS origin,
	st1.stop_id2 AS transfer,
	st2.stop_id2 AS destination,
	st1.departure_time AS departure,
	st1.arrival_time AS transfer_arrival,
	st2.departure_time AS transfer_departure,
	st2.arrival_time AS arrival,
	(strftime('%s',st2.departure_time)-strftime('%s',st1.arrival_time))/60 AS transfer_duration,
	(strftime('%s',st2.arrival_time)-strftime('%s',st1.departure_time))/60 AS one_transfer_duration
FROM direct AS st1
INNER JOIN (
	SELECT * 
	FROM direct 
	WHERE stop_id2 = (SELECT destination from parameters)
	) AS st2
ON st1.stop_id2 = st2.stop_id1 
	AND transfer_arrival < transfer_departure 
WHERE origin = (SELECT origin from parameters)
	AND one_transfer_duration < (SELECT trip_duration from parameters)
	AND transfer_duration < (SELECT transfer_duration from parameters)
	AND st1.trip_id IN
    	(SELECT trip_id FROM running_services)
    AND st2.trip_id IN
    	(SELECT trip_id FROM running_services)
limit 10
;



/*
Print all direct possibilities
*/

SELECT 
	*
FROM DIRECT 
WHERE stop_id2 = (SELECT destination from parameters)
AND stop_id1 = (SELECT origin from parameters)
AND (strftime('%s',arrival_time)-strftime('%s',departure_time))/60 < (SELECT trip_duration from parameters)
ORDER BY trip_duration
limit 10
;

/*
Print all possibilities with one transfer
*/


SELECT 
	ot1.trip1,
	ot1.trip2,
	ot1.origin,
	ot1.transfer,
	ot1.destination,
	ot1.departure,
	ot1.transfer_arrival,
	ot1.transfer_departure,
	ot1.arrival,
	ot1.transfer_duration,
	ot1.one_transfer_duration
FROM ONE_TRANSFER AS ot1
INNER JOIN (
	SELECT 
		trip1,
		trip2,
		MIN(one_transfer_duration) AS one_transfer_duration
	FROM ONE_TRANSFER
	GROUP BY trip1, trip2
	) as ot2
ON ot1.trip1 = ot2.trip1
AND ot1.trip2 = ot2.trip2
AND ot1.one_transfer_duration = ot2.one_transfer_duration
ORDER BY ot1.one_transfer_duration
limit 10
;


