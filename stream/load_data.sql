---- Transforming loaded JSON data on a schedule ----

CREATE DATABASE DML;

-- Landing table to store raw JSOn data - data could be loaded via Snowpipe
CREATE OR REPLACE TABLE raw (var variant);

-- A stream to capture inserts to the landing table - a task will consume a set of columns from this stream
CREATE OR REPLACE STREAM raw_check ON TABLE raw;
-- Second stream to capture inserts into the landing table - a second task will consume another set of columns from this stream
CREATE OR REPLACE STREAM raw_check2 ON TABLE raw;

-- A table to store the names of office visitors identified in the raw data
CREATE OR REPLACE TABLE names (id INT, first_name STRING, last_name STRING);
-- A table to store the visitation dates of office visitors identified in the raw data
CREATE OR REPLACE TABLE visits (id INT, dt DATE);

-- A task to insert new records from raw_check into the names table every minute
 CREATE OR REPLACE TASK raw_to_names
 WAREHOUSE = COMPUTE_WH
 SCHEDULE = '1 minute'
 WHEN
 SYSTEM$STREAM_HAS_DATA('raw_check')
 AS
 MERGE INTO names n
    USING (SELECT var:id id, var:fname fname, var:lname lname FROM raw_check) r1 ON n.id = to_number(r1.id)
    WHEN MATCHED THEN UPDATE SET n.first_name = r1.fname, n.last_name = r1.lname
    WHEN NOT MATCHED THEN INSERT (id, first_name, last_name) VALUES (r1.id, r1.fname, r1.lname);

-- Second task to merge visitation records from the raw_check2 stream into the visits table every minute
CREATE OR REPLACE TASK raw_to_visits
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('raw_check2')
AS
MERGE INTO visits v
    USING (SELECT var:id id, var:visit_dt visit_dt FROM raw_check2) r2 ON v.id = to_number(r2.id)
    WHEN MATCHED THEN UPDATE SET v.dt = r2.visit_dt
    WHEN NOT MATCHED THEN INSERT (id, dt) VALUES (r2.id, r2.visit_dt);

-- Resume both tasks
ALTER TASK raw_to_names RESUME;
ALTER TASK raw_to_visits RESUME;

-- Insert a set of records into the landing table
INSERT INTO raw
    SELECT parse_json(column1)
    FROM VALUES
    ('{"id": "123","fname": "Jane","lname": "Smith","visit_dt": "2019-09-17"}'),
    ('{"id": "456","fname": "Peter","lname": "Williams","visit_dt": "2019-09-17"}');

SELECT * FROM raw_check;
SELECT * FROM raw_check2;

SELECT * FROM raw;
SELECT * FROM names;
SELECT * FROM visits;

SELECT SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2024_08');
COPY INTO <table>
  FROM @~/<file>.json
  FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = true);