create or replace database mydatabase;

 use schema mydatabase.public;

  create or replace temporary table cities (
    continent varchar default null,
    country varchar default null,
    city variant default null
  );

create or replace warehouse mywarehouse with
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;

use warehouse mywarehouse;

CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format
  TYPE = parquet;

CREATE OR REPLACE TEMPORARY STAGE sf_tut_stage
FILE_FORMAT = sf_tut_parquet_format;

PUT file://C:\temp\load\cities.parquet @sf_tut_stage;

copy into cities
 from (select $1:continent::varchar,
              $1:country:name::varchar,
              $1:country:city::variant
      from @sf_tut_stage/cities.parquet.gz);

copy into @sf_tut_stage/out/parquet_
from (select continent,
             country,
             c.value::string as city
     from cities, lateral flatten(input => city) c)
  file_format = (type = 'parquet')
  header = true;

select t.$1 from @sf_tut_stage/out/ t;

REMOVE @sf_tut_stage/cities.parquet;

DROP DATABASE IF EXISTS mydatabase;
DROP WAREHOUSE IF EXISTS mywarehouse;

SHOW WAREHOUSES;