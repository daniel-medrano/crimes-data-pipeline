-- Script that creates the schemas and tables needed, the lines used to fill the tables manually are commented.
-- Schema Creation

CREATE SCHEMA RAW;
CREATE SCHEMA TRANSFORMED;
CREATE SCHEMA CONSUMPTION;

-- Table Creation
-- Raw Crimes Table
CREATE TABLE RAW.CRIMES (
    COL1 VARCHAR,
    COL2 VARCHAR,
    COL3 VARCHAR,
    COL4 VARCHAR,
    COL5 VARCHAR,
    COL6 VARCHAR,
    COL7 VARCHAR,
    COL8 VARCHAR,
    COL9 VARCHAR,
    COL10 VARCHAR,
    COL11 VARCHAR
);
-- Raw data from CSV files is copied to a table.
-- COPY INTO RAW.CRIMES
-- FROM (SELECT 
--     $1 AS CATEGORY, 
--     $2 AS MODALITY, 
--     $3 AS DATE, 
--     $4 AS TARGET_TYPE, 
--     $5 AS TARGET_DESCRIPTION, 
--     $6 AS AGE_CATEGORY, 
--     $8 AS NATIONALITY, 
--     $9 AS PROVINCE, 
--     $10 AS CANTON, 
--     $11 AS DISTRICT, 
--     $12 AS MISMATCH
-- FROM @PUBLIC.MY_STAGE);

-- Transformed Crimes Table

CREATE TABLE TRANSFORMED.CRIMES (
    CRIME_CATEGORY VARCHAR(25),
    CRIME_MODALITY VARCHAR(250),
    CRIME_DATE DATE,
    TARGET_TYPE VARCHAR(250),
    TARGET_DESCRIPTION VARCHAR(250),
    VICTIM_AGE_CATEGORY VARCHAR(250),
    VICTIM_NATIONALITY VARCHAR(250),
    CRIME_PROVINCE VARCHAR(250),
    CRIME_CANTON VARCHAR(250),
    CRIME_DISTRICT VARCHAR(250)
);

-- INSERT INTO TRANSFORMED.CRIMES SELECT 
--     COL1 AS CRIME_CATEGORY, 
--     COL2 AS CRIME_MODALITY, 
--     to_date(COL3) AS CRIME_DATE,
--     COL4 AS TARGET_TYPE, 
--     left(COL5, charindex('[', COL5) - 1) AS TARGET_DESCRIPTION, 
--     trim(upper(COL6)) AS VICTIM_AGE_CATEGORY,
--     upper(COL7) AS VICTIM_NATIONALITY, 
--     COL8 AS CRIME_PROVINCE, 
--     COL9 AS CRIME_CANTON, 
--     COL10 AS CRIME_DISTRICT 
-- FROM RAW.CRIMES
-- WHERE COL11 IS NULL
-- UNION ALL
-- SELECT 
--     COL1 AS CRIME_CATEGORY, 
--     COL2 AS CRIME_MODALITY, 
--     to_date(COL3) AS CRIME_DATE,
--     COL4 AS TARGET_TYPE, 
--     left(COL5, charindex('[', COL5) - 1) AS TARGET_DESCRIPTION, 
--     trim(upper(COL6)) AS VICTIM_AGE_CATEGORY,
--     concat(COL8,' ',COL7) AS VICTIM_NATIONALITY, 
--     COL9 AS CRIME_PROVINCE, 
--     COL10 AS CRIME_CANTON, 
--     COL11 AS CRIME_DISTRICT 
-- FROM RAW.CRIMES
-- WHERE COL11 IS NOT NULL;

-- Sequences Used as Unique Keys (IDs) for Dimension Tables

CREATE SEQUENCE CONSUMPTION.MODALITY_ID;
CREATE SEQUENCE CONSUMPTION.TARGET_ID;
CREATE SEQUENCE CONSUMPTION.DEMOGRAPHICS_ID;
CREATE SEQUENCE CONSUMPTION.GEOGRAPHICS_ID;
CREATE SEQUENCE CONSUMPTION.CRIME_ID;

-- Modality Dimension Table

CREATE TABLE CONSUMPTION.MODALITY_DIMENSION (
    MODALITY_ID INT DEFAULT CONSUMPTION.MODALITY_ID.NEXTVAL,
    MODALITY VARCHAR(250),
    CATEGORY VARCHAR(25)
);

-- INSERT INTO CONSUMPTION.MODALITY_DIMENSION (MODALITY,CATEGORY)
-- SELECT DISTINCT CRIME_MODALITY, CRIME_CATEGORY 
-- FROM TRANSFORMED.CRIMES
-- ORDER BY CRIME_CATEGORY, CRIME_MODALITY;

-- Date Dimension Table

CREATE TABLE CONSUMPTION.DATE_DIMENSION (
    DATE_ID VARCHAR(8),
    DATE DATE,
    DAY_WEEK INT,
    MONTH INT,
    MONTH_NAME VARCHAR(10),
    YEAR INT,
    TRIMESTER INT,
    QUARTER INT,
    SEMESTER INT
);

-- INSERT INTO CONSUMPTION.DATE_DIMENSION (DATE_ID,DATE,DAY_WEEK,MONTH,MONTH_NAME,YEAR,TRIMESTER,QUARTER,SEMESTER) 
-- SELECT DISTINCT 
--     concat(year(CRIME_DATE),
--         CASE 
--             WHEN length(month(CRIME_DATE)) = 1 THEN concat('0',month(CRIME_DATE))
--             ELSE to_varchar(month(CRIME_DATE))
--         END,
--         CASE 
--             WHEN length(day(CRIME_DATE)) = 1 THEN concat('0',day(CRIME_DATE))
--             ELSE to_varchar(day(CRIME_DATE))
--         END) AS DATE_ID,
--     CRIME_DATE AS DATE,
--     dayofweek(CRIME_DATE) AS DAY_WEEK,
--     month(CRIME_DATE) AS MONTH,
--     CASE MONTH
--         WHEN 1 THEN 'ENERO'
--         WHEN 2 THEN 'FEBRERO'
--         WHEN 3 THEN 'MARZO'
--         WHEN 4 THEN 'ABRIL'
--         WHEN 5 THEN 'MAYO'
--         WHEN 6 THEN 'JUNIO'
--         WHEN 7 THEN 'JULIO'
--         WHEN 8 THEN 'AGOSTO'
--         WHEN 9 THEN 'SEPTIEMBRE'
--         WHEN 10 THEN 'OCTUBRE'
--         WHEN 11 THEN 'NOVIEMBRE'
--         WHEN 12 THEN 'DICIEMBRE'
--     END AS MONTH_NAME,
--     year(CRIME_DATE) AS YEAR,
--     quarter(CRIME_DATE) AS TRIMESTER,
--     CASE
--         WHEN MONTH >= 1 AND MONTH <= 4  THEN 1
--         WHEN MONTH >= 5 AND MONTH <= 8  THEN 2
--         WHEN MONTH >= 9 AND MONTH <= 12  THEN 3
--     END AS QUARTER,
--     CASE
--         WHEN MONTH >= 1 AND MONTH <= 6  THEN 1
--         WHEN MONTH >= 6 AND MONTH <= 12  THEN 2    
--     END AS SEMESTER
-- FROM TRANSFORMED.CRIMES;

-- Target Dimension Table

CREATE TABLE CONSUMPTION.TARGET_DIMENSION (
    TARGET_ID INT DEFAULT CONSUMPTION.TARGET_ID.NEXTVAL,
    TARGET_TYPE VARCHAR(250),
    TARGET_DESCRIPTION VARCHAR(250)
);

-- INSERT INTO CONSUMPTION.TARGET_DIMENSION (TARGET_TYPE,TARGET_DESCRIPTION) 
-- SELECT DISTINCT TARGET_TYPE, TARGET_DESCRIPTION 
-- FROM TRANSFORMED.CRIMES
-- ORDER BY TARGET_TYPE, TARGET_DESCRIPTION;

-- Victim Demographics Dimension Table

CREATE TABLE CONSUMPTION.VICTIM_DEMOGRAPHICS_DIMENSION (
    DEMOGRAPHICS_ID INT DEFAULT CONSUMPTION.DEMOGRAPHICS_ID.NEXTVAL,
    AGE_CATEGORY VARCHAR(250),
    NATIONALITY VARCHAR(250)
);

-- INSERT INTO CONSUMPTION.VICTIM_DEMOGRAPHICS_DIMENSION (AGE_CATEGORY,NATIONALITY)
-- SELECT DISTINCT VICTIM_AGE_CATEGORY, VICTIM_NATIONALITY 
-- FROM TRANSFORMED.CRIMES
-- ORDER BY VICTIM_AGE_CATEGORY, VICTIM_NATIONALITY;

-- Geographics Dimension Table

CREATE TABLE CONSUMPTION.GEOGRAPHICS_DIMENSION (
    GEOGRAPHICS_ID INT DEFAULT CONSUMPTION.GEOGRAPHICS_ID.NEXTVAL,
    DISTRICT VARCHAR(250),
    CANTON VARCHAR(250),
    PROVINCE VARCHAR(250)
);

-- INSERT INTO CONSUMPTION.GEOGRAPHICS_DIMENSION (DISTRICT,CANTON,PROVINCE)
-- SELECT DISTINCT CRIME_DISTRICT, CRIME_CANTON, CRIME_PROVINCE 
-- FROM TRANSFORMED.CRIMES
-- ORDER BY CRIME_PROVINCE, CRIME_CANTON, CRIME_DISTRICT;

-- Crimes Fact Table

CREATE TABLE CONSUMPTION.CRIMES (
    CRIME_ID INT DEFAULT CONSUMPTION.CRIME_ID.NEXTVAL,
    MODALITY_ID INT,
    DATE_ID VARCHAR(8),
    TARGET_ID INT,
    DEMOGRAPHICS_ID INT,
    GEOGRAPHICS_ID INT
);

-- INSERT INTO CONSUMPTION.CRIMES (MODALITY_ID, DATE_ID, TARGET_ID, DEMOGRAPHICS_ID, GEOGRAPHICS_ID) 
-- SELECT MODALITY_ID, DATE_ID, TARGET_ID, DEMOGRAPHICS_ID, GEOGRAPHICS_ID
-- FROM TRANSFORMED.CRIMES TC
-- INNER JOIN CONSUMPTION.MODALITY_DIMENSION CMD
--     ON TC.CRIME_CATEGORY = CMD.CATEGORY AND TC.CRIME_MODALITY = CMD.MODALITY
-- INNER JOIN CONSUMPTION.DATE_DIMENSION CDD
--     ON TC.CRIME_DATE = CDD.DATE
-- INNER JOIN CONSUMPTION.TARGET_DIMENSION CTD
--     ON TC.TARGET_TYPE = CTD.TARGET_TYPE AND TC.TARGET_DESCRIPTION = CTD.TARGET_DESCRIPTION
-- INNER JOIN CONSUMPTION.VICTIM_DEMOGRAPHICS_DIMENSION CVDD
--     ON TC.VICTIM_AGE_CATEGORY = CVDD.AGE_CATEGORY AND TC.VICTIM_NATIONALITY = CVDD.NATIONALITY
-- INNER JOIN CONSUMPTION.GEOGRAPHICS_DIMENSION CGD
--     ON TC.CRIME_PROVINCE = CGD.PROVINCE AND TC.CRIME_CANTON = CGD.CANTON AND TC.CRIME_DISTRICT = CGD.DISTRICT;