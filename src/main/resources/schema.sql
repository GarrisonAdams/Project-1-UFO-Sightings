CREATE TABLE byHourTable (
    keyType varchar,
    cases int
);

CREATE TABLE byMonthTable (
    keyType int,
    cases int
);

CREATE TABLE byYearTable (
    keyType varchar,
    cases int
);


CREATE TABLE byCountryTable (
    keyType varchar,
    cases int
);

CREATE TABLE byShapeTable (
    keyType varchar,
    cases int
);

CREATE TABLE byDurationTable (
    keyType varchar,
    cases int
);

CREATE TABLE byStateTable (
    keyType varchar,
    cases int
);


DELETE FROM byShapeTable;
DELETE FROM byDurationTable;
DELETE FROM byHourTable;
DELETE FROM byMonthTable; 
DELETE FROM byYearTable;
DELETE FROM byCountryTable;
DELETE FROM byStateTable;

