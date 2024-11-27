-- Create a database. A database automatically includes a schema named 'public'.

CREATE OR REPLACE DATABASE mydatabase;

/* Create target tables for CSV and JSON data. The tables are temporary, meaning they persist only for the duration of the user session and are not visible to other users. */

CREATE OR REPLACE TEMPORARY TABLE mycsvtable (
  id INTEGER,
  last_name STRING,
  first_name STRING,
  company STRING,
  email STRING,
  workphone STRING,
  cellphone STRING,
  streetaddress STRING,
  city STRING,
  postalcode STRING);

CREATE OR REPLACE TEMPORARY TABLE myjsontable (
  json_data VARIANT);

-- Create a warehouse

CREATE OR REPLACE WAREHOUSE mywarehouse WITH
  WAREHOUSE_SIZE='X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED=TRUE;

  CREATE OR REPLACE FILE FORMAT mycsvformat
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  SKIP_HEADER = 1;

  CREATE OR REPLACE FILE FORMAT myjsonformat
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

  CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat;

  CREATE OR REPLACE STAGE my_json_stage
  FILE_FORMAT = myjsonformat;

  PUT file://C:\temp\load\contacts*.csv @my_csv_stage AUTO_COMPRESS=TRUE;
  PUT file://C:\temp\load\contacts.json @my_json_stage AUTO_COMPRESS=TRUE;

  LIST @my_csv_stage;
  LIST @my_json_stage;

  COPY INTO mycsvtable
  FROM @my_csv_stage/contacts1.csv.gz
  FILE_FORMAT = (FORMAT_NAME = mycsvformat)
  ON_ERROR = 'skip_file';
  SELECT * FROM mycsvtable;

  COPY INTO myjsontable
  FROM @my_json_stage/contacts.json.gz
  FILE_FORMAT = (FORMAT_NAME = myjsonformat)
  ON_ERROR = 'skip_file';