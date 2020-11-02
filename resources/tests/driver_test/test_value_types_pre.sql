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
intervalVal INTERVAL DAY TO SECOND(4),
intervalYMVal INTERVAL YEAR TO MONTH,
timeVal TIME,
timeTZVal TIMETZ,
varBinVal VARBINARY,
uuidVal UUID,
lVarCharVal LONG VARCHAR(65536),
lVarBinaryVal LONG VARBINARY(65536),
binaryVal BINARY,
numericVal NUMERIC
);

INSERT INTO full_type_table VALUES(
true, 123, 3.141, 'a', 'test values', '1999-Jan-08', '2019-08-04T00:45:19.843913-04:00', '2019-08-04T00:45:19.843913-04:00',
'17910y 1h 3m 6s 5msecs 57us ago', '1 2', '04:05:06.789', '04:05:06-8:00 PM',
HEX_TO_BINARY('beefdead')::VARBINARY, '372fd680-6a72-4003-96b0-10bbe78cd635', 'longer var char',
HEX_TO_BINARY('deadbeef')::LONG VARBINARY,HEX_TO_BINARY('baadf00d'), 1.2345);

INSERT INTO full_type_table VALUES(null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null);
