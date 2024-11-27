CREATE OR REPLACE DATABASE MULTI_JOIN;

-- Create multiple tables with matching column values.
CREATE TABLE birds (
  id number,
  common varchar(100),
  class varchar(100)
);

CREATE TABLE sightings (
  d date,
  loc varchar(100),
  b_id number,
  c number
);

-- Create a view that queries the tables with a join.
CREATE VIEW bird_sightings AS
SELECT b.id AS id,
       b.common AS common_name,
       b.class AS classification,
       s.d AS date,
       s.loc AS location,
       s.c AS count
FROM birds b
INNER JOIN sightings s ON b.id = s.b_id;

SELECT * FROM bird_sightings;

-- Create a stream on the view.
CREATE STREAM bird_sightings_s ON VIEW bird_sightings;

-- Insert values into the tables.
INSERT INTO birds
VALUES
    (1,'Scarlet Tanager','P. olivacea'),
    (14,'Mallard','A. platyrhynchos'),
    (48,'Spotted Sandpiper','A. macularius'),
    (92,'Great Blue Heron','A. herodias');

INSERT INTO sightings
VALUES
    (current_date(),'Gibson Island',1,4),
    (current_date(),'Lake Los Pajaro',14,12),
    (current_date(),'Lake Los Pajaro',92,12),
    (current_date(),'Gibson Island',14,21),
    (current_date(),'Gibson Island',92,5);

-- Query the stream.
-- The stream displays a record for each row added to the view.
SELECT * FROM bird_sightings_s;

-- Consume the stream records in a DML statement (INSERT, MERGE, etc.).

-- Delete a row from the birds table.
DELETE FROM birds WHERE id = 14;

-- Query the stream.
-- The stream displays two records for the single DELETE operation.
SELECT * FROM bird_sightings_s;
