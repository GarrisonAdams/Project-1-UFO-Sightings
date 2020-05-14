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

CREATE TABLE inCountryTable (
    keyType varchar,
    cases int
);

CREATE TABLE byCountryTable (
    keyType varchar,
    cases int
);

CREATE TABLE byStateTable (
    keyType varchar,
    cases int
);

CREATE TABLE inStateTable (
    keyType varchar,
    cases int
);

DELETE FROM byHourTable;
DELETE FROM byMonthTable; 
DELETE FROM byYearTable;
DELETE FROM inCountryTable;
DELETE FROM byCountryTable;
DELETE FROM inStateTable;
DELETE FROM byStateTable;

