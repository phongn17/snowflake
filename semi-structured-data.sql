CREATE OR REPLACE DATABASE mydatabase;

USE SCHEMA mydatabase.public;

CREATE OR REPLACE TABLE raw_source (
  SRC VARIANT);

CREATE OR REPLACE WAREHOUSE mywarehouse WITH
  WAREHOUSE_SIZE='X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED=TRUE;

USE WAREHOUSE mywarehouse;

CREATE OR REPLACE STAGE my_stage
  URL = 's3://snowflake-docs/tutorials/json';

COPY INTO raw_source
  FROM @my_stage/server/2.6/2016/07/15/15
  FILE_FORMAT = (TYPE = JSON);

SELECT * FROM raw_source;

SELECT src:device_type FROM raw_source;
SELECT src:events FROM raw_source;
SELECT src:device_type::string AS device_type FROM raw_source;
SELECT value:f::number AS F, value:rv::string AS RV FROM raw_source, LATERAL FLATTEN(INPUT => SRC:events);
SELECT src:device_type::string,
    src:version::string,
    VALUE
FROM
    raw_source,
    LATERAL FLATTEN( INPUT => SRC:events );
CREATE OR REPLACE TABLE flattened_source AS
  SELECT
    src:device_type::string AS device_type,
    src:version::string     AS version,
    VALUE                   AS src
  FROM
    raw_source,
    LATERAL FLATTEN( INPUT => SRC:events );
SELECT * FROM flattened_source;
create or replace table events as
  select
    src:device_type::string                             as device_type
  , src:version::string                                 as version
  , value:f::number                                     as f
  , value:rv::variant                                   as rv
  , value:t::number                                     as t
  , value:v.ACHZ::number                                as achz
  , value:v.ACV::number                                 as acv
  , value:v.DCA::number                                 as dca
  , value:v.DCV::number                                 as dcv
  , value:v.ENJR::number                                as enjr
  , value:v.ERRS::number                                as errs
  , value:v.MXEC::number                                as mxec
  , value:v.TMPI::number                                as tmpi
  , value:vd::number                                    as vd
  , value:z::number                                     as z
  from
    raw_source
  , lateral flatten ( input => SRC:events );
SELECT * FROM events;
ALTER TABLE events ADD CONSTRAINT pk_DeviceType PRIMARY KEY (device_type, rv);
insert into raw_source
  select
  PARSE_JSON ('{
    "device_type": "cell_phone",
    "events": [
      {
        "f": 79,
        "rv": "786954.67,492.68,3577.48,40.11,343.00,345.8,0.22,8765.22",
        "t": 5769784730576,
        "v": {
          "ACHZ": 75846,
          "ACV": 098355,
          "DCA": 789,
          "DCV": 62287,
          "ENJR": 2234,
          "ERRS": 578,
          "MXEC": 999,
          "TMPI": 9
        },
        "vd": 54,
        "z": 1437644222811
      }
    ],
    "version": 3.2
  }');
insert into events
select
      src:device_type::string
    , src:version::string
    , value:f::number
    , value:rv::variant
    , value:t::number
    , value:v.ACHZ::number
    , value:v.ACV::number
    , value:v.DCA::number
    , value:v.DCV::number
    , value:v.ENJR::number
    , value:v.ERRS::number
    , value:v.MXEC::number
    , value:v.TMPI::number
    , value:vd::number
    , value:z::number
    from
      raw_source
    , lateral flatten( input => src:events )
    where not exists
    (select 'x'
      from events
      where events.device_type = src:device_type
      and events.rv = value:rv);
insert into raw_source
  select
  parse_json ('{
    "device_type": "web_browser",
    "events": [
      {
        "f": 79,
        "rv": "122375.99,744.89,386.99,12.45,78.08,43.7,9.22,8765.43",
        "t": 5769784730576,
        "v": {
          "ACHZ": 768436,
          "ACV": 9475,
          "DCA": 94835,
          "DCV": 88845,
          "ENJR": 8754,
          "ERRS": 567,
          "MXEC": 823,
          "TMPI": 0
        },
        "vd": 55,
        "z": 8745598047355
      }
    ],
    "version": 8.7
  }');

insert into events
select
      src:device_type::string
    , src:version::string
    , value:f::number
    , value:rv::variant
    , value:t::number
    , value:v.ACHZ::number
    , value:v.ACV::number
    , value:v.DCA::number
    , value:v.DCV::number
    , value:v.ENJR::number
    , value:v.ERRS::number
    , value:v.MXEC::number
    , value:v.TMPI::number
    , value:vd::number
    , value:z::number
    from
      raw_source
    , lateral flatten( input => src:events )
    where not exists
    (select 'x'
      from events
      where events.device_type = src:device_type
      and events.version = src:version
      and events.f = value:f
      and events.rv = value:rv
      and events.t = value:t
      and events.achz = value:v.ACHZ
      and events.acv = value:v.ACV
      and events.dca = value:v.DCA
      and events.dcv = value:v.DCV
      and events.enjr = value:v.ENJR
      and events.errs = value:v.ERRS
      and events.mxec = value:v.MXEC
      and events.tmpi = value:v.TMPI
      and events.vd = value:vd
      and events.z = value:z);