------------------------------------------
create database my_db_09;
create schema my_schema_09;

-- list user stage;
list @~;

-- list table stage;
list @%customer_csv;


-- list named stages
show stages;

use database my_db;
use schema my_schema;
show stages;

list @weather_nyc;


use database my_db_08;
use schema my_schema_08;
show stages;

list @my_stgMY_DB.MY_SCHEMA;


--------------------------------------------------------------
-- create internal stage

create stage my_db_09.my_schema_09.stg03 comment = 'this is my demo internal stage';

-- snowsql command to load into user stage:
-- put file://~/Desktop/tmp/cities.csv @~/ch-09/;

show stages;
show stages like '%03';
list @~;

list @~ pattern='.*.';


--------------------------------------------------------------
-- loading file to table stage
select 
    I_ITEM_SK, 
    I_ITEM_ID, 
    replace(I_ITEM_DESC, ',', '') I_ITEM_DESC,
    I_CURRENT_PRICE, 
    I_WHOLESALE_COST, 
    I_CLASS_ID, 
    I_CLASS, 
    I_CATEGORY_ID, 
    I_CATEGORY, 
    I_MANUFACT_ID, 
    I_MANUFACT, 
    I_SIZE,  
    I_COLOR, 
    I_UNITS, 
    I_CONTAINER, 
    I_PRODUCT_NAME
from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.ITEM limit 10;

create table my_db_09.my_schema_09.item (
    I_ITEM_SK integer, 
    I_ITEM_ID string, 
    I_ITEM_DESC string,
    I_CURRENT_PRICE decimal, 
    I_WHOLESALE_COST decimal, 
    I_CLASS_ID integer, 
    I_CLASS string, 
    I_CATEGORY_ID integer, 
    I_CATEGORY string, 
    I_MANUFACT_ID integer, 
    I_MANUFACT string, 
    I_SIZE string,  
    I_COLOR string, 
    I_UNITS string, 
    I_CONTAINER string, 
    I_PRODUCT_NAME string
);

select * from item;

put file://~/Desktop/tmp/item.csv @%item/ch-09/;

-- list table stage;
use database my_db_09;
use schema my_schema_09;

list @%item;
list @%item/ch-09/;

remove @~;
remove @%item/ch-09/;

-- drop stage stg03;

-- put command not applicable for external stages

--------------------------------------------------------------

-- Load and query data using from table stage
create or replace table customer_parquet_ff(
    v_data variant 
)
stage_file_format = (type = parquet);
