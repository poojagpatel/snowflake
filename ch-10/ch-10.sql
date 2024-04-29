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

remove @stg_10/history/customer_history.csv;


----------------------------------------------------
-- step 4

-- create pipe

create or replace pipe my_pipe_10
as copy into customer 
from @stg_10/delta/;

desc pipe my_pipe_10;

-- once the pipe is created it has to be resumed

-- check if delta folder has any data
list @stg_10/delta/;


-- resume pipe
-- as you resume the pipe, it becomes active 
alter pipe my_pipe_10 refresh;


----------------------------------------------------
-- step 5
-- monitor the pipe using pipe_status
select system$pipe_status('my_pipe_10');

-- use validate_pipe_load to check load history
select * from table (
    validate_pipe_load(
        pipe_name => 'my_pipe_10',
        start_time => dateadd(hour, -1, current_timestamp())
    )
);

-- pipe will not load data unless a notification is available in snowflake queue
-- will do this via python code



----------------------------------
-- download query sample data for customer

select 
    C_CUSTKEY, C_NAME, replace(C_ADDRESS, ',', '') C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, replace(C_COMMENT, ',', '') C_COMMENT
from 
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
limit 10 offset 10;


