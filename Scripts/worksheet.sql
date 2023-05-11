CREATE SEQUENCE seq_01;

SELECT seq_01.nextval AS a, seq_01.nextval AS b;

CREATE TABLE PRUEBA_01 AS SELECT SEQ_01.NEXTVAL, ROOT_DEPTH_NAME FROM GARDEN_PLANTS.VEGGIES.ROOT_DEPTH;

SELECT * FROM PRUEBA_01;

SELECT S.NEXTVAL, RD.RANGE_MIN FROM GARDEN_PLANTS.VEGGIES.ROOT_DEPTH RD, TABLE(GETNEXTVAL(SEQ_01)) S;

SELECT trim('      HOLA    ');

create table 

create table DC23 as select 
    $1 AS CATEGORY, 
    $2 AS MODALITY, 
    $3::date AS DATE, 
    $4 AS TARGET_TYPE, 
    left($5, charindex('[', $5) - 1) AS TARGET_DESCRIPTION, 
    TRIM(UPPER($6)) AS AGE_CATEGORY, 
    $8 AS NATIONALITY, 
    $9 AS PROVINCE, 
    $10 AS CANTON, 
    $11 AS DISTRICT 
from @MY_STAGE limit 10;

select 
    $1 AS CATEGORY, 
    $2 AS MODALITY, 
    $3::date AS DATE, 
    $4 AS TARGET_TYPE, 
    left($5, charindex('[', $5) - 1) AS TARGET_DESCRIPTION, 
    TRIM(UPPER($6)) AS AGE_CATEGORY, 
    $8 AS NATIONALITY, 
    $9 AS PROVINCE, 
    $10 AS CANTON, 
    $11 AS DISTRICT 
from @MY_STAGE limit 10;

select left('hola como', charindex(' ', 'hola como') - 1);

select count(*) from @MY_STAGE;

select count(*) from @MY_STAGE where $5 like '%  [%]';

select count(*) from dc23;

select $12 from @MY_STAGE where $12 is not null;

insert into dc23 select 
    $1 AS CATEGORY, 
    $2 AS MODALITY, 
    $3::date AS DATE, 
    $4 AS TARGET_TYPE, 
    left($5, charindex('[', $5) - 1) AS TARGET_DESCRIPTION, 
    TRIM(UPPER($6)) AS AGE_CATEGORY, 
    $8 AS NATIONALITY, 
    $9 AS PROVINCE, 
    $10 AS CANTON, 
    $11 AS DISTRICT 
from @MY_STAGE
where $12 is null
union all
select 
    $1 AS CATEGORY, 
    $2 AS MODALITY, 
    $3::date AS DATE, 
    $4 AS TARGET_TYPE, 
    left($5, charindex('[', $5) - 1) AS TARGET_DESCRIPTION, 
    TRIM(UPPER($6)) AS AGE_CATEGORY, 
    concat($9,' ',$8) AS NATIONALITY, 
    $10 AS PROVINCE, 
    $11 AS CANTON, 
    $12 AS DISTRICT 
from @MY_STAGE
where $12 is not null;

truncate table dc23;
