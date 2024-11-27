USE DATABASE append_only;

-- Create a source table.
create or replace table t(id int, name string);

-- Create a standard stream on the source table.
create or replace  stream delta_s on table t;

-- Create an append-only stream on the source table.
create or replace  stream append_only_s on table t append_only=true;

-- Insert 3 rows into the source table.
insert into t values (0, 'charlie brown');
insert into t values (1, 'lucy');
insert into t values (2, 'linus');

-- Delete 1 of the 3 rows.
delete from t where id = '0';

-- The standard stream removes the deleted row.
select * from delta_s order by id;

-- The append-only stream does not remove the deleted row.
select * from append_only_s order by id;

-- Create a table to store the change data capture records in each of the streams.
create or replace  table t2(id int, name string, stream_type string default NULL);

-- Insert the records from the streams into the new table, advancing the offset of each stream.
insert into t2(id,name,stream_type) select id, name, 'delta stream' from delta_s;
insert into t2(id,name,stream_type) select id, name, 'append_only stream' from append_only_s;

-- Update a row in the source table.
update t set name = 'sally' where name = 'linus';

-- The standard stream records the update operation.
select * from delta_s order by id;

-- The append-only stream does not record the update operation.
select * from append_only_s order by id;
