---- Unloading data on a schedule ----

USE DATABASE DML;

-- Landing table
CREATE OR REPLACE TABLE raw2 (id INT, type STRING);
-- A stream to feed the unload command
CREATE OR REPLACE STREAM raw2_check ON TABLE raw2;
-- A task to excecute COPY every minute
CREATE OR REPLACE TASK unload
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
WHEN
    SYSTEM$STREAM_HAS_DATA('raw2_check')
AS
COPY INTO @%raw2/raw2_check FROM raw2_check OVERWRITE = true;
-- Resume the task
ALTER TASK unload RESUME;
-- Insert raw data into the landing table
INSERT INTO raw2 VALUES (3, 'processed');
-- Query the CDC record in the table stream
SELECT * FROM raw2_check;
-- Wait for the task to run
CALL SYSTEM$WAIT(70);
-- Verify yje COPY statement unloaded a data file into the table stage
ls @%raw2;

CREATE TASK exttable_refresh_task
WAREHOUSE=mywh
SCHEDULE='5 minutes'
  AS
ALTER EXTERNAL TABLE mydb.myschema.exttable REFRESH;