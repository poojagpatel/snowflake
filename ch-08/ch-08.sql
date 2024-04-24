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



