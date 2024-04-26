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
-- important to specify file format during table creation for unnamed stages
create or replace table customer_parquet(
    v_data variant 
)
stage_file_format = (type = parquet);

select * from customer_parquet;
remove @%customer_parquet;
list @%customer_parquet;
-----------------

-- querying data using $ notation - used when data need to be queried on a file etc.
-- for any semi structured data $1 is the only way to access all the columns

select 
    metadata$filename
    , metadata$file_row_number
    , $1:C_CUSTKEY::varchar
    , $1:C_NAME::varchar
    , $1:C_ACCTBAL::decimal(10,2)
    , $1:C_ADDRESS::varchar
    , $1:C_PHONE::varchar
    , $1:C_NATIONKEY::varchar
    , $1:C_MKTSEGMENT::varchar
    , $1:C_COMMENT::varchar
from @%customer_parquet;

copy into customer_parquet 
from @%customer_parquet;

select * from customer_parquet;

-- once data is loaded, you can see the load_history table to see the history
-- if you try to load the same file again and again, it will not load the data
-- copy + tables have metadata which remembers last 64 days of data laod history
-- you can use 'Force = True | False', to reload the same data file
-- by default the Force flag is False

copy into customer_parquet 
from @%customer_parquet
force = True;


-- copy history table
select * from snowflake.account_usage.copy_history limit 10;

-- load history table
select * from snowflake.account_usage.load_history limit 10;

-- stages - does not show unnamed stages
select * from snowflake.account_usage.stages limit 10;
