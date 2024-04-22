-- set context and warehouse
use role sysadmin;

-- create database
create database my_db
comment = 'this my demo db';

show databases;

select current_role(), current_database();


-- create schema
create schema my_schema
comment = 'this is my schema under my_db';


show schemas;
select current_role(), current_database(), current_schema();


-- explict setting of my db and schema
use database my_db;
use schema my_schema;
select current_role(), current_database(), current_schema(), current_warehouse();


-- create tables - number data types
drop table if exists my_table;
create table my_table(
    id int autoincrement,
    num number,
    num_10_1 number(10, 1),
    decimal_20_2 decimal(20, 2),
    numeric numeric(30, 3),
    int int, 
    integer integer
);

describe table my_table;

select get_ddl('table', 'my_table');

use warehouse my_wh;

-- insertion

insert into my_table(num, num_10_1, decimal_20_2, numeric, int, integer) 
    values(10, 22.2, 33.33, 123456789, 987654321, 12112);


insert into my_table(num, num_10_1, decimal_20_2, numeric, int, integer) 
        values(20, 22.2, 33.33, 123456789, 987654321, 12112), 
            (30, 22.2, 33.33, 123456789, 987654321, 12112);

select * from my_table;


-- create table - char data types
drop table if exists my_text_table;
create table my_text_table (
        id int autoincrement,
        v varchar,
        v50 varchar(50),
        c char,
        c10 char(10),
        s string,
        s20 string(20),
        t text,
        t30 text(30)
    );

describe table my_text_table;

insert into my_text_table(v, v50, c, c10, s, s20, t, t30) 
     values('a','b','c','d','e','f','g','h');

insert into my_text_table(v, v50, c, c10, s, s20, t, t30) 
values('a','b','ch','d','e','f','g','h');
select * from my_text_table;


-- boolean tables
create or replace table my_boolean_table(
    b boolean,
    n number,
    s string
);

desc table my_boolean_table;
insert into my_boolean_table 
    values  (true, 1, 'yes'), 
            (false, 0, 'no'), 
            (null, null, null);
            
select * from my_boolean_table;


-- time stamp table
drop table if exists my_ts_tablel;
create or replace table my_ts_table(
    today_date date default current_date(),
    now_time time default current_time(),
    now_ts timestamp default current_timestamp()
);
      
-- lets desc the table
desc table my_ts_table;

-- insert one record 
insert into my_ts_table (today_date, now_time, now_ts) values (current_date, current_time, current_timestamp);
insert into my_ts_table (now_time, now_ts) values (current_time, current_timestamp);

select * from my_ts_table;

alter session set timezone = 'Japan';
alter session set timestamp_output_format = 'YYYY-MM-DD HH24:MI:SS.FF';


-----------------------------------------------------

-- create table as select

create table my_ctas as select * from my_db.my_schema.my_table;

select * from my_ctas;

-- load data using select as statement

insert into my_ctas
(num, num_10_1, decimal_20_2, numeric, int, integer) 
select 
    num, num_10_1, decimal_20_2, numeric, int, integer
from my_table;


select * from my_table;