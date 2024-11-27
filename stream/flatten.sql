CREATE DATABASE flatten;

---- Flatten array sql

create or replace transient table emp01(
    id number,
    first_name varchar,
    last_name varchar,
    designation varchar,
    certifications array
);

-- 1st records with one certification
insert into emp01 
select 1, 'Alexander', 'Kostas','Snowflake Developer',array_construct('SnowPro Core');

-- 2nd recocrd with two certification
insert into emp01 
select 2,'Pierre', 'Dupont','Sr. Snowflake Developer',array_construct('SnowPro Core','SnowPro Adv DE');

-- 3rd recocrd with three certification
insert into emp01 
select 3,'Isabella', 'Rossi','Snowflake Architect',array_construct('SnowPro Core','SnowPro Adv DE','SnowPro Architect');

-- check the table data
select * from emp01 ;


select flatten_tbl.value::varchar as certification 
from table(flatten (input => array_construct('SnowPro Core','SnowPro Adv DE','SnowPro Architect'))) flatten_tbl
order by 1;

select emp.first_name, emp.last_name, cert.value::varchar as cert_name from 
    emp01 emp,
    lateral flatten (input => emp.certifications) cert;

---- Flatten object sql ----

create or replace transient table emp02(
    id number,
    first_name varchar,
    last_name varchar,
    designation varchar,
    certifications object
);

-- 1st records with one certification
insert into emp02 
select  1, 'Alexander', 'Kostas','Snowflake Developer',
        object_construct('SnowPro Core','790');

-- 2nd recocrd with two certification
insert into emp02 
select  2,'Pierre', 'Dupont','Sr. Snowflake Developer',
        object_construct('SnowPro Core','890','SnowPro Adv DE','810');

-- 3rd recocrd with three certification
insert into emp02 
select  3,'Isabella', 'Rossi','Snowflake Architect',
        object_construct('SnowPro Core','950','SnowPro Adv DE','780','SnowPro Architect','900');

-- check the table data
select * from emp02 ;


-- lets understand the flatten table function first..
select flatten_tbl.value::varchar as certification from table(flatten (input => array_construct('SnowPro Core','SnowPro Adv DE','SnowPro Architect'))) flatten_tbl
order by 1;


select emp.first_name, emp.last_name, cert.value::varchar as cert_name from 
    emp02 emp,
    lateral flatten (input => emp.certifications) cert;

select emp.first_name, emp.last_name, cert.key::varchar as cert_name,
cert.value::varchar as cert_marks from 
    emp02 emp,
    lateral flatten (input => emp.certifications) cert;

---- Flatten array & object together sql ----

create or replace transient table camera(
    name varchar,
    brand varchar,
    front_camera varchar,
    rear_camera object,
    dim_lwh array   
);


-- 1st records with one certification
insert into camera 
select  'iPhone 12', 'Apple','12Mp',
        object_construct('Primary/Std','14MP', 'Wide Angle','16MP'),
        array_construct('16.49','8.96','2.82')
        ;
       
select 
    camera.name,
    camera.brand,
    camera.front_camera,
    rear.key::varchar as rear_camera_type,
    rear.value::varchar as rear_camera_px,
    dim.value::number as dimension
from 
    camera,
    lateral flatten (input => camera.rear_camera) rear,
    lateral flatten (input => camera.dim_lwh) dim
    ;
    
select 
    camera.name,
    camera.brand,
    dim.value::number as dimension
from 
    camera,
    lateral flatten (input => camera.dim_lwh) dim
    ;

---- Flatten JSON ----

create or replace transient table emp04(
    json_data variant   
);


--1st record
insert into emp04
select parse_json('
                  {
                    "id":1, "name":"Alexander Kostas", "designation":"Snowflake Developer",
                    "certifications":
                    [
                        {"name":"SnowPro Core","score":"790"}
                    ]
                  }
                  ');
-- 2nd record
insert into emp04
select parse_json('
                  {
                    "id":2, "name":"Pierre Dupont", "designation":"Sr. Snowflake Developer",
                    "certifications":
                    [
                      {"name":"SnowPro Adv DE","score":"810"},      
                      {"name":"SnowPro Core","score":"890"}
                        
                    ]
                  }
                  ');
-- 3rd record
insert into emp04
select parse_json('
                  {
                    "id":3, "name":"Isabella Rossi", "designation":"Snowflake Architect",
                    "certifications":
                    [
                  {"name":"SnowPro Architect","score":"900"},      
                  {"name":"SnowPro Adv DE","score":"780"},                  
                  {"name":"SnowPro Core","score":"950"}  
                    ]
                  }
                  ');


-- let see how data looks like
select json_data from emp04;

-- extract basic elements
select 
    json_data:id::number as emp_id,
    json_data:name::varchar as emp_name,
    json_data:designation::varchar as designation,
    json_data:certifications
from emp04;

-- lets extract tradition way
select 
    json_data:id::number as emp_id,
    json_data:name::varchar as emp_name,
    json_data:designation::varchar as designation,
    json_data:certifications[0].name::varchar as first_cert_name,
    json_data:certifications[0].score::varchar as first_cert_score
from emp04;

-- lets use flatten
select 
    json_data:id::number as emp_id,
    json_data:name::varchar as emp_name,
    json_data:designation::varchar as designation,
    cert.value:name::varchar as cert_name,
    cert.value:score::varchar as cert_score
    
from emp04 emp,
lateral flatten (input => emp.json_data:certifications) cert

---- Flatten function & performance ----

{
    "employees": [
      {
        "name": "John Doe",
        "programming_skills": [
          {
            "language": "Java",
            "proficiency": "expert",
            "experience": {
              "version": "8",
              "years": 5
            }
          },
          {
            "language": "Python",
            "proficiency": "intermediate",
            "experience": {
              "version": "3",
              "years": 3
            }
          }
        ],
        "database_proficiency": [
          {
            "technology": "Oracle",
            "proficiency": "expert",
            "experience": {
              "version": "19c",
              "years": 7
            }
          },
          {
            "technology": "DB2",
            "proficiency": "intermediate",
            "experience": {
              "version": "11.5",
              "years": 2
            }
          }
        ],
        "cloud_experience": ["AWS"]
      },
      {
        "name": "Jane Smith",
        "programming_skills": [
          {
            "language": "Java",
            "proficiency": "expert",
            "experience": {
              "version": "11",
              "years": 8
            }
          },
          {
            "language": "Python",
            "proficiency": "advanced",
            "experience": {
              "version": "3.9",
              "years": 5
            }
          },
          {
            "language": "C++",
            "proficiency": "beginner",
            "experience": {
              "version": "14",
              "years": 1
            }
          }
        ],
        "database_proficiency": [
          {
            "technology": "Oracle",
            "proficiency": "advanced",
            "experience": {
              "version": "12c",
              "years": 6
            }
          },
          {
            "technology": "MySQL",
            "proficiency": "intermediate",
            "experience": {
              "version": "8.0",
              "years": 3
            }
          }
        ],
        "cloud_experience": ["AWS", "GCP"]
      }
    ]
  }

select 
    json:id::number as id,
    json:full_name::varchar as name,
    to_date(json:joining_date::varchar,'DD-MM-YYYY') as join_of_birth,
    to_date(json:dob::varchar,'DD-MM-YYYY') as date_of_birth,
    json:department::varchar as department,
    json:designation::varchar as designation,
    json:salary::number as salary,
    json:city_of_birth::varchar as city_of_birth,
    json:country_of_birth::varchar as country_of_birth,
    array_size(json:programming_skills) as prg_exp_cnt,
    json:programming_skills[0].language,
    json:programming_skills[0].proficiency,
    json:programming_skills[0].experience.version,
    json:programming_skills[0].experience.years,
    json:programming_skills[1].language,
    json:programming_skills[1].proficiency,
    json:programming_skills[1].experience.version,
    json:programming_skills[1].experience.years,
    array_size(json:database_proficiency) as db_exp_cnt,
    json:database_proficiency[0].technology,
    json:database_proficiency[0].proficiency,
    json:database_proficiency[0].experience.version,
    json:database_proficiency[0].experience.years,
    json:database_proficiency[1].technology,
    json:database_proficiency[1].proficiency,
    json:database_proficiency[1].experience.version,
    json:database_proficiency[1].experience.years,
    array_size(json:cloud_experience) as cloud_exp_cnt,
    json:cloud_experience[0],
    json:cloud_experience[1],
    json:certifications.snowflake
from emp_json_1k;

-- with flatten
select 
    e.json:id::number as id,
    e.json:full_name::varchar as name,
    to_date(e.json:dob::varchar,'DD-MM-YYYY') as dob,
    e.json:designation::varchar as designation,
    e.json:salary::number as salary,
    prog.value as prog_value,
    db.value as db_value,
    cert.value as cert_value
from emp_json_1k e,
lateral flatten (input => e.json:programming_skills) prog,
lateral flatten (input => e.json:cloud_experience) db,
lateral flatten (input => e.json:certifications, outer => true) cert
;

---- Flatten function with outer parameter ----

create or replace transient table emp_json_outer(
    emp_json_data variant   
)
stage_file_format = (type = 'JSON');

list @%emp_json_outer;

-- run select
select * from emp_json_outer;

--run a copy command
copy into emp_json_outer;

-- with flatten
select 
    emp_json_data:id::number as id,
    emp_json_data:full_name::varchar as name,
    prog_skill.value:language::varchar as lang,
    prog_skill.value:proficiency::varchar as proficiency
from emp_json_outer,
lateral flatten (input => emp_json_data:programming_skills,
                outer => true) prog_skill
order by id;


select 
    json:id::number as id,
    json:full_name::varchar as name,
    to_date(json:joining_date::varchar,'DD-MM-YYYY') as join_of_birth,
    to_date(json:dob::varchar,'DD-MM-YYYY') as date_of_birth,
    json:department::varchar as department,
    json:designation::varchar as designation,
    json:salary::number as salary,
    json:city_of_birth::varchar as city_of_birth,
    json:country_of_birth::varchar as country_of_birth,
    array_size(json:programming_skills) as prg_exp_cnt,
    json:programming_skills[0].language,
    json:programming_skills[0].proficiency,
    json:programming_skills[0].experience.version,
    json:programming_skills[0].experience.years,
    json:programming_skills[1].language,
    json:programming_skills[1].proficiency,
    json:programming_skills[1].experience.version,
    json:programming_skills[1].experience.years,
    array_size(json:database_proficiency) as db_exp_cnt,
    json:database_proficiency[0].technology,
    json:database_proficiency[0].proficiency,
    json:database_proficiency[0].experience.version,
    json:database_proficiency[0].experience.years,
    json:database_proficiency[1].technology,
    json:database_proficiency[1].proficiency,
    json:database_proficiency[1].experience.version,
    json:database_proficiency[1].experience.years,
    array_size(json:cloud_experience) as cloud_exp_cnt,
    json:cloud_experience[0],
    json:cloud_experience[1],
    json:certifications.snowflake
from emp_json_50k;

-- with flatten
select 
    e.json:id::number as id,
    e.json:full_name::varchar as name,
    to_date(e.json:dob::varchar,'DD-MM-YYYY') as dob,
    e.json:designation::varchar as designation,
    e.json:salary::number as salary,
    prog.value,
    db.value:technology,
    cert.value
from emp_json e,
lateral flatten (input => e.json:programming_skills) prog,
lateral flatten (input => e.json:database_proficiency) db,
lateral flatten (input => e.json:certifications outer =>true) cert;

---- Flatten function with JSON data ----

create or replace transient table emp_json(
    emp_json_data variant   
)
stage_file_format = (type = 'JSON');

list @%emp_json;

select * from emp_json;

copy into emp_json;

-- lets query the json using colon notation;

select 
    emp_json_data:id::number as id,
    emp_json_data:full_name::varchar as name,
    to_date(emp_json_data:joining_date::varchar,'DD-MM-YYYY') as join_of_birth,
    to_date(emp_json_data:dob::varchar,'DD-MM-YYYY') as date_of_birth,
    emp_json_data:department::varchar as department,
    emp_json_data:designation::varchar as designation,
    emp_json_data:salary::number as salary,
    emp_json_data:city_of_birth::varchar as city_of_birth,
    emp_json_data:country_of_birth::varchar as country_of_birth,
    emp_json_data:programming_skills[1].language,
    emp_json_data:programming_skills[1].proficiency,
    emp_json_data:programming_skills[1].experience.version,
    emp_json_data:programming_skills[].experience.years
from emp_json;

-- with flatten
select 
    emp_json_data:id::number as id,
    emp_json_data:full_name::varchar as name,
    to_date(emp_json_data:joining_date::varchar,'DD-MM-YYYY') as join_of_birth,
    to_date(emp_json_data:dob::varchar,'DD-MM-YYYY') as date_of_birth,
    emp_json_data:department::varchar as department,
    emp_json_data:designation::varchar as designation,
    emp_json_data:salary::number as salary,
    emp_json_data:city_of_birth::varchar as city_of_birth,
    emp_json_data:country_of_birth::varchar as country_of_birth,
    prog_skill.value:language::varchar as lang,
    prog_skill.value:proficiency::varchar as proficiency,
    
    db_skill.value:technology::varchar as technology
from emp_json,
lateral flatten (input => emp_json_data:programming_skills) prog_skill,
lateral flatten (input => emp_json_data:database_proficiency) db_skill
where 
    (prog_skill.value:language::varchar) = 'Java' and 
    (prog_skill.value:proficiency::varchar) = 'Beginner' and 
    (db_skill.value:technology::varchar) = 'DB2';


list @%emp_json;

copy into emp_json;

select * from emp_json;

-- understand other parameters
select 
    prog_skill.*
from emp_json,
lateral flatten (
        input => emp_json_data:programming_skills,
        path => 'programming_skills.experience'
            ) prog_skill;



list @%emp_json;

copy into emp_json;

select * from emp_json;

-- Another employee data with certification
-- will explore the path input parameter
select 
    cert.*
from emp_json,
lateral flatten (
        input => emp_json_data:certifications,
        path => 'java',
        recursive => true,
        mode => 'Object',
        outer => false
            ) cert;