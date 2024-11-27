CREATE OR REPLACE DATABASE etl;

-- Create a staging table that stores raw JSON data
CREATE OR REPLACE TABLE data_staging (
  raw variant);

-- Create a stream on the staging table
CREATE OR REPLACE STREAM data_check ON TABLE data_staging;

-- Create 2 production tables to store transformed JSON data in relational columns
CREATE OR REPLACE TABLE data_prod1 (
    id number(8),
    ts TIMESTAMP_TZ
    );

CREATE OR REPLACE TABLE data_prod2 (
    id number(8),
    color VARCHAR,
    num NUMBER
    );

-- Load JSON data into staging table using COPY statement, Snowpipe, or inserts
INSERT INTO data_staging (raw) SELECT parse_json($${
   "id": 7077,                        
   "x1": "2018-08-14T20:57:01-07:00", 
   "x2": [                            
     {                                
       "y1": "green",                 
       "y2": "35"                     
     }                                
   ]                                  
 }$$);

INSERT INTO data_staging (raw) SELECT parse_json($${                                    
   "id": 7078,                        
   "x1": "2018-08-14T21:07:26-07:00", 
   "x2": [                            
     {                                
       "y1": "cyan",                  
       "y2": "107"                    
     }                                
   ]                                  
 }$$);

SELECT * FROM data_staging;

--  Stream table shows inserted data
SELECT * FROM data_check;

-- Access and lock the stream
BEGIN;

-- Transform and copy JSON elements into relational columns in the production tables
INSERT INTO data_prod1 (id, ts)
    SELECT t.raw:id, to_timestamp_tz(t.raw:x1)
    FROM data_check t
    WHERE METADATA$ACTION = 'INSERT';

INSERT INTO data_prod2 (id, color, num)
SELECT t.raw:id, f.value:y1, f.value:y2
FROM data_check t, lateral flatten(input => raw:x2) f
WHERE METADATA$ACTION = 'INSERT';

-- Commit changes in the stream objects participating in the transaction
COMMIT;

SELECT * FROM data_prod1;

SELECT * FROM data_prod2;

SELECT * FROM data_check;
