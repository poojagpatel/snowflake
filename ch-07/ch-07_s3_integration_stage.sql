
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::250850880152:role/snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-lab-pp/weather_nyc/');


DESC INTEGRATION s3_integration;

use database my_db;
use schema my_db.my_schema;


CREATE STAGE weather_nyc
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://snowflake-lab-pp/weather_nyc/';


list @weather_nyc;


-- variant data
create table json_weather_data(v variant);
desc table json_weather_data;

copy into MY_DB.MY_SCHEMA.json_weather_data 
    from @weather_nyc 
    file_format = (type=json);


delete from json_weather_data;

select * from json_weather_data;

-- create view
create view json_weather_data_view as
    select
      v:time::timestamp as observation_time,
      v:city.id::int as city_id,
      v:city.name::string as city_name,
      v:city.country::string as country,
      v:city.coord.lat::float as city_lat,
      v:city.coord.lon::float as city_lon,
      v:clouds.all::int as clouds,
      v:main.temp::float as temp_avg,
      v:main.temp_min::float as temp_min,
      v:main.temp_max::float as temp_max,
      v:weather[0].main::string as weather,
      v:weather[0].description::string as weather_desc,
      v:weather[0].icon::string as weather_icon,
      v:wind.deg::float as wind_dir,
      v:wind.speed::float as wind_speed
    from json_weather_data
    where city_id = 5128581;


select * from json_weather_data_view;


 -- create external table
create or replace external table json_weather_data_et (
    time varchar AS (value:c1::varchar), 
    ....

)
with location=@nyc_weather
auto_refresh = false
file_format = (format_name = file_format)
;



