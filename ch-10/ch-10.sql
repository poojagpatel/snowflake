----------------------------------------------------
-- set context

create database my_db_10;
create schema my_db_10.my_schema_10;
use warehouse compute_wh;

alter session set query_tag = 'ch-10';

----------------------------------------------------

-- step 1
-- create table my_customer

create or replace table customer (
    cust_key number(38,0)
    ,name varchar(35)
    ,address varchar
    ,nation_key number(38,0)
    ,phone varchar(15)
    ,account_balance number(12,2)
    ,market_segment varchar
    ,comment varchar
);

select * from customer;

----------------------------------------------------
-- create simple csv file format as we will use csv as delta file

-- step 2

create or replace file format csv_ff
type = 'csv'
field_optionally_enclosed_by='\042'
SKIP_HEADER = 1;

show stages like '%_010';

create or replace stage stg_10
file_format = csv_ff
comment='this is my internal stage for ch10';

desc stage stg_10;

list @stg_10;

----------------------------------------------------
-- step 3
-- load into table

copy into customer from @stg_10/history;

select * from customer;
select count(*) from customer;






----------------------------------
-- download query sample data for customer

select 
    C_CUSTKEY, C_NAME, replace(C_ADDRESS, ',', '') C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, replace(C_COMMENT, ',', '') C_COMMENT
from 
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
limit 10;





