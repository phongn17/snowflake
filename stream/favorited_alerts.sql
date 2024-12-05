CREATE OR REPLACE DATABASE favorites;

CREATE OR REPLACE TABLE favorited_alerts_staging (
    raw variant
);

CREATE OR REPLACE TABLE favorited_alerts_index (subjectId VARCHAR(36));

INSERT INTO favorited_alerts_staging (raw) SELECT parse_json($${
   "subjectId": "4839b762-241d-40cb-97a3-f6f78e01fd78",                        
   "neighborhoodIds": [1, 2, 3], 
   "schoolIds": [4, 5, 6],
   "buildingIds": [7, 8, 9, 10]
 }$$);

INSERT INTO favorited_alerts_staging (raw) SELECT parse_json($${
   "subjectId": "a8a7d3ca-667a-49a8-9801-0ab87eb73aeb",                        
   "neighborhoodIds": [10, 20, 30], 
   "schoolIds": [40, 50, 60],
   "buildingIds": [70, 80, 90, 100]
 }$$);

SELECT * FROM favorited_alerts_staging;

CREATE OR REPLACE TABLE favorited_alerts (
    subjectId VARCHAR(36),
    neighborhoodIds ARRAY,
    schoolIds ARRAY,
    buildingIds ARRAY
    );

INSERT INTO favorited_alerts (subjectId, neighborhoodIds, schoolIds, buildingIds) 
SELECT
  UUID_STRING(), 
  ARRAY_CONSTRUCT(1, 2, 3),
  ARRAY_CONSTRUCT(4, 5, 6),
  ARRAY_CONSTRUCT(7, 8, 9, 10);

INSERT INTO favorited_alerts (subjectId, neighborhoodIds, schoolIds, buildingIds) 
SELECT
  UUID_STRING(), 
  ARRAY_CONSTRUCT(10, 20, 30),
  ARRAY_CONSTRUCT(40, 50, 60),
  ARRAY_CONSTRUCT(70, 80, 90, 100);

SELECT * FROM favorited_alerts;

SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();

CREATE OR REPLACE STORAGE INTEGRATION s3_favorited_alerts_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::205930619262:role/snowflake_favorited_alerts_access'
    STORAGE_AWS_OBJECT_ACL = 'bucket-owner-full-control'
    STORAGE_ALLOWED_LOCATIONS = ('s3://favorited-alerts/');

DESC INTEGRATION s3_favorited_alerts_int;

CREATE OR ALTER ROLE snowflake_favorited_alerts_access;
GRANT CREATE STAGE ON SCHEMA PUBLIC TO ROLE snowflake_favorited_alerts_access;
GRANT USAGE ON INTEGRATION s3_favorited_alerts_int TO ROLE snowflake_favorited_alerts_access;

USE Favorites.public;

CREATE OR REPLACE FILE FORMAT unload_json_format
    TYPE = json
    COMPRESSION = gzip;

CREATE OR REPLACE STAGE s3_favorited_alerts_stage
    STORAGE_INTEGRATION = s3_favorited_alerts_int
    URL = 's3://favorited-alerts/'
    FILE_FORMAT = unload_json_format;

COPY INTO @s3_favorited_alerts_stage/d1 FROM favorited_alerts_staging;

CREATE OR REPLACE FILE FORMAT unload_json_format_no_compression
    TYPE = json
    COMPRESSION = none;

CREATE OR REPLACE STAGE s3_favorited_alerts_stage_no_compression
    STORAGE_INTEGRATION = s3_favorited_alerts_int
    URL = 's3://favorited-alerts/'
    FILE_FORMAT = unload_json_format_no_compression;

COPY INTO @s3_favorited_alerts_stage_no_compression/d1 FROM favorited_alerts_staging;

DECLARE
  numSubjectIds INT;
BEGIN
  numSubjectIds := 1000000;
  FOR idx IN 1 TO numSubjectIds DO
    INSERT INTO favorited_alerts_index(subjectId) SELECT UUID_STRING();
  END FOR;
END;