create or replace database my_db_08 comment = 'database for chapter 8';
create schema my_schema_08 comment = 'schema for chapter 8';
use database my_db_08;
use schema my_schema_08;

------------------------------------------------------------
-- creating sequence
create or replace sequence seq_01 start = 1 increment = 1 comment = 'this is a trial sequence';
create or replace sequence seq_02 start = 1 increment = 2 comment = 'this is a trial sequence';
create or replace sequence seq_03 start = 0 increment = -2 comment = 'this is a trial sequence';

select seq_01.nextval, seq_02.nextval, seq_03.nextval;

-- use sequence in table
create or replace table my_tbl_seq(
    pk int autoincrement,
    seq1 int default seq_01.nextval,
    seq2 int default seq_02.nextval,
    seq3 int default seq_03.nextval,
    msg string
);

desc table my_tbl_seq;

select get_ddl('table', 'my_tbl_seq');

insert into my_tbl_seq(msg) values('msg-4');
select * from my_tbl_seq;

show sequences;

select get_ddl('sequence', 'seq_02');

------------------------------------------------------------
-- file formats

-- csv

create table customer_csv(
    cust_key integer,
    name string,
    address string,
    country_key string,
    phone string,
    acct_bal decimal,
    mkt_segment string,
    comment string
);

select * from customer_csv;

-- parquet

create table customer_parquet(
    data variant
);

select * from customer_parquet;

select seq, key, value from customer_parquet,
lateral flatten(input => data);

show file formats;

select get_ddl('file_format', 'CSV_FILE_FORMAT');

------------------------------------------------------------
-- stages
-- create internal stage using web ui

list @my_stg;

select count(*) from customer_parquet;
select * from customer_parquet;

copy into customer_parquet 
from @my_stg/parquet/ch-08_customer.parquet
file_format = (type=parquet);

copy into customer_parquet 
from @my_stg/parquet/ch-08_customer.parquet
file_format = (FORMAT_NAME=parquet_file_format)
FORCE = TRUE;

delete from customer_parquet;



------------------------------------------------------------
-- pipes
-- following is not working
create or replace pipe MY_DB_08.MY_SCHEMA_08.MY_PIPE auto_ingest=false comment='first pipe to load small chunk of data' as copy into MY_DB_08.MY_SCHEMA_08.customer_csv
    from @MY_DB_08.MY_SCHEMA_08.my_stg/csv
    pattern = 'ch-08*'
    ON_ERROR = CONTINUE
    file_format = (
        type=csv
        skip_header = 1
        );

show pipes;
select get_ddl('pipe', 'my_pipe');

select SYSTEM$PIPE_STATUS('my_pipe');



------------------------------------------------------------
-- stream

create or replace stream customer_stream
on table customer_csv;

select * from customer_csv;

copy into customer_csv
from @my_stg/csv/ch-08_customer_1.csv
on_error = continue
file_format = (type=csv);

select * from customer_stream;

delete from customer_csv where cust_key=60006;

update customer_csv 
set address = 'this is updated address'
where cust_key=60009;

show streams;
select get_ddl('stream', 'customer_stream');



------------------------------------------------------------
-- task

create or replace task my_task
    warehouse = compute_wh
    schedule = '5 minute'
as select current_date;




------------------------------------------------------------
-- rough
select * from  snowflake_sample_data.tpch_sf1.customer limit 10 offset 10;

select 
    c_custkey::string c_custkey,
    c_name::string c_name,
    c_address::string c_address,
    c_nationkey::string c_nationkey,
    c_phone::string c_phone,
    c_acctbal::string c_acctbal,
    c_mktsegment::string c_mktsegment,
    c_comment
from snowflake_sample_data.tpch_sf1.customer limit 10 offset 10;

