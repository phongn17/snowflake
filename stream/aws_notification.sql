CREATE NOTIFICATION INTEGRATION my_notification_int
    ENABLED = TRUE
    DIRECTION = OUTBOUND
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AWS_SNS
    AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:205930619262:snowflake_integration'
    AWS_SNS_ROLE_ARN = 'arn:aws:iam::205930619262:role/snowflake_sns_topic';

DESC NOTIFICATION INTEGRATION my_notification_int;

CREATE TASK mytask
    SCHEDULE = '5 MINUTE'
    ERROR_INTEGRATION = my_notification_int
    AS
    INSERT INTO data_staging(raw)
    VALUES
    (parse_json($${                                    
       "id": 7078,                        
       "x1": "2018-08-14T21:07:26-07:00", 
       "x2": [                            
         {                                
           "y1": "cyan",                  
           "y2": "107"                    
         }                                
       ]                                  
     }$$));

ALTER TASK mytask SET SUCCESS_INTEGRATION = my_notification_int;

DESC TASK mytask;

CREATE OR REPLACE FUNCTION addone(num int)
RETURNS int
LANGUAGE python
RUNTIME_VERSION = '3.8'
HANDLER = 'addone_py'
AS
$$
def addone_py(num):
    return num + 1
$$;

CREATE OR REPLACE TASK mytask2
    AFTER mytask
    AS
        SELECT addone(SYSTEM$GET_PREDECESSOR_RETURN_VALUE());

-- Returns a filtered table with rows that match the specified role
CREATE OR REPLACE PROCEDURE filterByRole(tableName VARCHAR, role VARCHAR)
    RETURNS TABLE(id NUMBER, name VARCHAR, role VARCHAR)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'filter_by_role'
    AS
$$
from snowflake.snowpark.functions import col

def filter_by_role(session, table_name, role):
    df = session.table(table_name)
    return df.filter(col("role") == role)
$$;

SHOW PARAMETERS LIKE 'USER_TASK_TIMEOUT_MS' IN TASK mytask;