SET AUTOCOMMIT = ON;

DROP TABLE IF EXISTS full_type_table;

CREATE TABLE full_type_table(
boolVal BOOLEAN,
intVal INT,
floatVal FLOAT,
charVal CHAR,
varCharVal VARCHAR(128),
dateVal DATE,
timestampVal TIMESTAMP,
timestampTZVal TIMESTAMPTZ,
intervalDayVal INTERVAL DAY,
intervalVal INTERVAL DAY TO SECOND(4),
intervalHourVal INTERVAL HOUR,
intervalHMVal INTERVAL HOUR TO MINUTE,
intervalHSVal INTERVAL HOUR TO SECOND,
intervalMinVal INTERVAL MINUTE,
intervalMSVal INTERVAL MINUTE TO SECOND,
intervalSecVal INTERVAL SECOND(2),
intervalDHVal INTERVAL DAY TO HOUR,
intervalDMVal INTERVAL DAY TO MINUTE,
intervalYearVal INTERVAL YEAR,
intervalYMVal INTERVAL YEAR TO MONTH,
intervalMonthVal INTERVAL MONTH,
timeVal TIME,
timeTZVal TIMETZ,
varBinVal VARBINARY,
uuidVal UUID,
lVarCharVal LONG VARCHAR(65536),
lVarBinaryVal LONG VARBINARY(65536),
binaryVal BINARY,
numericVal NUMERIC(40,18)
);

INSERT INTO full_type_table VALUES(
true, 123, 3.141, 'a', 'test values', '1999-Jan-08', '2019-08-04 00:45:19.843913', '2019-08-04 00:45:19.843913 -04:00',
'1y 10m', '17910y 1h 3m 6s 5msecs 57us ago', '3 days 2 hours', '1 3', '1y 15 mins 20 sec', '15 mins 20 sec',
'1y 5 mins 20 sec', '2 days 12 hours 15 mins 1235 milliseconds', '2 days 12 hours 15 mins ago',
'2 days 12 hours 15 mins ago', '1y 10m', '1 2', '1y 10m', '04:05:06.789', '04:05:06-8:00 PM',
HEX_TO_BINARY('beefdead')::VARBINARY, '372fd680-6a72-4003-96b0-10bbe78cd635', 'longer var char',
HEX_TO_BINARY('deadbeef')::LONG VARBINARY,HEX_TO_BINARY('baadf00d'), 1.2345);

INSERT INTO full_type_table VALUES(null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null);
