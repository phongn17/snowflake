-- Stream on a view that calls a non-deterministic sql function
CREATE OR REPLACE DATABASE non_deterministic;

-- Create a table.
CREATE TABLE ndf (
  c1 number
);

-- Create a view that queries the table and also returns the CURRENT_USER and CURRENT_TIMESTAMP values for the query 
-- transaction.
CREATE VIEW ndf_v AS
SELECT CURRENT_USER() AS u,
       CURRENT_TIMESTAMP() AS ts,
       c1 AS num
FROM ndf;

-- Create a stream on the view.
CREATE STREAM ndf_s ON VIEW ndf_v;

-- User peter inserts rows into table ndf.
INSERT INTO ndf
VALUES
    (1),
    (2),
    (3);

-- User marie inserts rows into table ndf.
INSERT INTO ndf
VALUES
    (4),
    (5),
    (6);

-- User PNGUYEN queries the stream.
-- The stream returns the username for the user.
-- The stream also returns the current timestamp for the query transaction in each row,
-- NOT the timestamp when each row was inserted.
SELECT * FROM ndf_s;

-- User MARIE queries the stream.
-- The stream returns the username for the user
-- and the current timestamp for the query transaction in each row.
SELECT * FROM ndf_s;
