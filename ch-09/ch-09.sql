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
    I_ITEM_SK varchar, 
    I_ITEM_ID varchar, 
    I_ITEM_DESC varchar,
    I_CURRENT_PRICE varchar, 
    I_WHOLESALE_COST varchar, 
    I_CLASS_ID varchar, 
    I_CLASS varchar, 
    I_CATEGORY_ID varchar, 
    I_CATEGORY varchar, 
    I_MANUFACT_ID varchar, 
    I_MANUFACT varchar, 
    I_SIZE varchar,  
    I_COLOR varchar, 
    I_UNITS varchar, 
    I_CONTAINER varchar, 
    I_PRODUCT_NAME varchar
);

drop table item;


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

--------------------------------------------------------------
-- file format and copy command

create or replace file format my_parquet_ff type = 'parquet';
create or replace file format my_csv_ff type = 'csv' SKIP_HEADER = 1;
create or replace file format my_json_ff type = 'json';

show file formats;

-- explicitly specifying flie format while creating stage when you want to enforce type of files in the stage
create or replace stage stg_csv
file_format = my_csv_ff
comment = 'stage will use csv file format';

create or replace stage stg_none
comment = 'no file format attached';

list @stg_csv;
list @stg_none;
select * from @stg_csv/ch-09_customer.parquet;
select * from @stg_csv/item.csv;

-- copy using stage where file format is attached
copy into item
from @stg_csv;

-- copy using stage where file format is not attached
copy into item
from @stg_none
file_format = (format_name = my_csv_ff);

select * from item;


--------------------------------------------------------------
-- file format and copy command

-- Query stage

create or replace table my_customer (
    cust_key number(38,0),
    name varchar(30),
    address varchar,
    nation_key number(38,0),
    phone varchar,
    account_balance number(12,2),
    market_segment varchar,
    comment varchar
);

select * from my_customer;

create or replace stage my_stg
file_format = my_parquet_ff
comment = 'stage will use parquet file format';

list @my_stg/my_data;

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
from @my_stg/my_data/;


--------------------------------------------------------------
-- file pattern can be used to copy data from the stage

copy into t1
from @%t1/region/state/city/2024/04/27/
files = ('mydata1.csv', 'mydata2.csv');

copy into t1
from @%t1/region/state/city/2024/04/27/
pattern= '.*mydata[^[0-9]{1,3}$$].csv';

copy into t1
from @%t1/region/state/city/2024/04/27/
files = ('mydata1.csv', 'mydata2.csv');


list @stg_csv;

select $1, $2, $3 from @stg_csv;

-- check for erroneous files
copy into item
from @stg_csv
validation_mode = 'RETURN_ERRORS';


-- check handful rows before loading as dry run
copy into item
from @stg_csv
validation_mode = 'RETURN_5_ROWS';




