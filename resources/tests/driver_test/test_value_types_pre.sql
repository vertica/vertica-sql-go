SET AUTOCOMMIT = ON;

DROP TABLE IF EXISTS full_type_table;

CREATE TABLE full_type_table(
boolVal BOOLEAN,
intVal INT,
floatVal FLOAT,
charVal CHAR,
varCharVal VARCHAR(128),
timestampVal TIMESTAMP,
timestampTZVal TIMESTAMPTZ,
varBinVal VARBINARY,
uuidVal UUID,
lVarCharVal LONG VARCHAR(65536),
lVarBinaryVal LONG VARBINARY(65536),
binaryVal BINARY,
numericVal NUMERIC
);

INSERT INTO full_type_table VALUES(
true, 123, 3.141, 'a', 'test values', '2019-08-04T00:45:19.843913-04:00', '2019-08-04T00:45:19.843913-04:00',
HEX_TO_BINARY('beefdead')::VARBINARY, '372fd680-6a72-4003-96b0-10bbe78cd635', 'longer var char',
HEX_TO_BINARY('deadbeef')::LONG VARBINARY,HEX_TO_BINARY('baadf00d'), 1.2345);

INSERT INTO full_type_table VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null);
