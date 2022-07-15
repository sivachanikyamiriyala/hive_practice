create DATABASE if not EXISTS chanikya 
comment 'DATABASE name is chanikya'
location '/user/miriyalathriveni8198/chanikya'
with dbproperties('key1'='value1')
;

use chanikya ;

describe DATABASE chanikya ;

describe DATABASE extended chanikya ;

select current_database() ;


create database if not exists sample ;

drop database if exists sample ;

drop database if exists sample cascade ;
drop DATABASE if EXISTS restrict ;

alter database chanikya set dbproperties('key1'='value1');
alter database chanikya set dbproperties set owner user hadoop ;
alter database chanikya set dbpropertiesset owner role hadoop ;

create table if not exists student1(name string,id int,course string,year int)
row format delimited 
fields terminated by '\001'
lines terminated by '\n'
stored as textfile 
;

create table if not exists student2(name string,id int,course string,year int)
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;
create external table if not exists student3(name string,id int,course string,year int)
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;
create temporary table if not exists student4(name string,id int,course string,year int)
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

 select * from sivam.student2;
+----------------+--------------+------------------+----------------+--+
| student2.name  | student2.id  | student2.course  | student2.year  |
+----------------+--------------+------------------+----------------+--+
| arun           | 1            | cse              | 1              |
| sunil          | 2            | cse              | 1              |
| raj            | 3            | cse              | 1              |
| naveen         | 4            | cse              | 1              |
| venki          | 5            | cse              | 2              |
| prasad         | 6            | cse              | 2              |
| sudha          | 7            | cse              | 2              |
| ravi           | 1            | mech             | 1              |
| raju           | 2            | mech             | 1              |
| roja           | 3            | mech             | 1              |
| anil           | 4            | mech             | 2              |
| rani           | 5            | mech             | 2              |
| anvith         | 6            | mech             | 2              |
| madhu          | 7            | mech             | 2              |
+----------------+--------------+------------------+----------------+--+


insert into table chanikya.student1 select * from sivam.student2 ;


create external TABLE if not EXISTS `STUDENT_TEXT1`(name string,id int,course string,year int)
COMMENT 'table name is STUDENT_TEXT1' 
row format delimited
fields terminated by '|'
lines terminated by '\n' 
stored as textfile 
;

create external TABLE if not EXISTS `STUDENT_TEXT2`(name string,id int,course string,year int)
COMMENT 'table name is STUDENT_TEXT1' 
row format delimited
fields terminated by '\t'
lines terminated by '\n' 
stored as textfile 
location '/user/miriyalathriveni8198/chanikya/sampledir'
;

alter TABLE `student_text2` SET location 'hdfs://cxln1.c.thelab-240901.internal:8020/user/miriyalathriveni8198/sampledir/' ;


create TABLE if not exists `STUDENT`
(`NAME` string comment 'student name',
`ID` int COMMENT 'student id',
`COURSE` string COMMENT 'student course',
`YEAR` int COMMENT 'student year'
)
COMMENT 'table name is student'
row format delimited 
fields terminated by '\t' 
lines terminated by '\n'
stored as textfile 
location '/user/miriyalathriveni8198/sampledir/'
tblproperties('key1'='value1')
;

+--------------------------------------------------------------------------------+--+
|                                   DFS Output                                   |
+--------------------------------------------------------------------------------+--+
| venkat#math,phy,chem#math:40,phy:23,chem:60#sec,500004,hyderabad#0,75.6        |
| kumar#math,phy,chem#math:50,phy:20,chem:40#lbngar,500005,hyderabad#1,78        |
| rahul#math,phy,chem#math:30,phy:22,chem:30#hitech,500003,hyderabad#2,hi:hello  |
| anil#math,phy,chem#math:13,phy:30,chem:60#srnagar,500002,hyderabad#3,1:rank    |
| arun#math,phy,chem#math:10,phy:20,chem:50#ameerpet,500001,hyderabad#4,true     |
+--------------------------------------------------------------------------------+--+


CREATE TABLE IF NOT EXISTS `STUDENTDATA`
(
`NAME` string comment 'student name',
`SUBJECTS` array<string> comment 'student subjects',
`MARKS` map<string,int> comment 'student marks',
`ADDRESS` struct<loc:string comment 'student location',pincode:int comment 'student pincode',city:string comment 'student city'> comment 'student address',
`DETAILS` uniontype<double,int,array<string>,struct<a:int,b:string>,boolean> comment 'student details'
)
COMMENT 'Table name is studentdata'
row format delimited 
fields terminated by '#'
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n' 
stored as textfile 
location '/user/miriyalathriveni8198/sampledir2/'
;


----------------------------------------------------------------------------------------------------------------------------
select t1.* from 
(select * from table1) t1 
join
(select id,max(date) as maxdate from 
(select * from table1) t2
group by id ) s 
on t1.id=s.id and t1.date=s.maxdate 
;

-----------------------------------------------------------------------------------------------------------------------------
select t1.* from 
(select * from table1) t1 
left outer join table2 t2 
on t1.name=t2.name 
where t2.name is null 
;

-----------------------------------------------------------------------------------------------------------------------------


create database if not exists sivam
comment 'database name is sivam'
with dbproperties('key1'='value1')
;

use sivam; 

arun    1       cse     1
sunil   2       cse     1
raj     3       cse     1
naveen  4       cse     1
venki   5       cse     2

create table if not exists student1(name string,id int,course string,year int)
row format delimited
fields terminated by '\001'
lines terminated by '\n'
stored as textfile 
;

create table if not exists student2(name string,id int,course string,year int)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

create external table if not exists student3(name string,id int,course string,year int)
row format delimited
fields terminated by '\t' 


create database if not EXISTS sivam
COMMENT 'database name is sivam'
location '/user/miriyalathriveni8198/sivam'
with dbproperties('key1'='value1')
;

use sivam ;

CREATE TABLE if not EXISTS student1(name string,id int ,course string,year int)
comment 'table name is student1'
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

CREATE TABLE if not EXISTS student2(name string,id int ,course string,year int)
COMMENT 'table name is student2'
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

CREATE external TABLE if not EXISTS student3(name string,id int ,course string,year int)
COMMENT 'table name is student3'
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;


CREATE temporary table if not EXISTS student4(name string,id int ,course string,year int)
COMMENT 'table name is student4'
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

CREATE TABLE if not EXISTS studenttext1(name string,id int ,course string,year int)
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

CREATE external TABLE if not EXISTS studenttext2(name string,id int ,course string,year int)
row format delimited 
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
location '/user/miriyalathriveni8198/chan'
;

CREATE external TABLE if not exists `STUDENT`
(`student_name` string COMMENT 'student name',
`student_id` int comment 'student id',
`student_course` string comment 'student course',
`student_year` int comment 'student year'
)
COMMENT 'table name is student'
row format delimited
fields terminated by '\t' 
lines terminated by '\n'
stored as textfile 
location '/user/miriyalathriveni8198/chan'
;


venkat#math,phy,chem#math:40,phy:23,chem:60#sec,500004,hyderabad#0,75.6
kumar#math,phy,chem#math:50,phy:20,chem:40#lbngar,500005,hyderabad#1,78
rahul#math,phy,chem#math:30,phy:22,chem:30#hitech,500003,hyderabad#2,hi:hello
anil#math,phy,chem#math:13,phy:30,chem:60#srnagar,500002,hyderabad#3,1:rank
arun#math,phy,chem#math:10,phy:20,chem:50#ameerpet,500001,hyderabad#4,true


CREATE TABLE if not EXISTS `STUDENTDATA`
(
`name` string COMMENT 'student name',
`subjects` array<string> comment 'student subjects',
`marks` map<string,int> COMMENT 'student marks',
address struct<loc:string comment 'student location',pincode:int comment 'student pincode',city:string comment 'student city'> comment 'student address',
details uniontype<double,int,array<string>,struct<a:int,b:string>,boolean> comment 'student details'
)
COMMENT 'table name is studentdata'
row format delimited
fields terminated by '#'
collection items terminated by ','
map keys terminated by ':'
lines terminated by '\n'
stored as textfile 
;


create table if not exists `STUDENT_PARTITION`
(`NAME` string,
`id` int)
partitioned by 
(
`course` string,
`year` int
)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile ;


create table if not exists users(name string,id int)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;


CREATE TABLE if not EXISTS users1(name string,id int)
clustered by (id) into 4 buckets ;

CREATE TABLE if not EXISTS users2(name string,id int)
clustered by (id) sorted by (id desc) into 5 buckets ;


create table if not exists student_old(name string,id string,course string,year string)
row format serde 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
with serdeproperties("input.regex"="([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)")
;

create table if not exists student_new(name string,id int,course string,year int)
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties("input.regex"="([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)")
;


hadoop  2010    10.0
hadoop  2011    40.5
hadoop  2012    60.0
hive    2012    40.5
hive    2013    20.0
pig     2014    30.0

create TABLE if not exists sales(name string,year int,percentage float)
row format delimited
fields terminated by '\t' 
lines terminated by '\n' 
stored as textfile 
;

hadoop  1       1000
hive    2       500
pig     3       750
hbase   4       600
spark   5       1500

CREATE TABLE if not EXISTS products(name string,id tinyint,year smallint)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile 
;

select /* + mapjoin(s)*/ p.*,s.* from products p join sales s on p.name=s.name ;


CREATE TABLE if not exists student_textfile(name string,id tinyint,course string,year smallint)
stored as textfile ;

CREATE TABLE if not exists student_sequencefile(name string,id tinyint,course string,year smallint)
stored as sequencefile ;

CREATE TABLE if not exists student_avro(name string,id tinyint,course string,year smallint)
stored as avro ;

CREATE TABLE if not exists student_rcfile(name string,id tinyint,course string,year smallint)
stored as rcfile ;

CREATE TABLE if not exists student_orc(name string,id tinyint,course string,year smallint)
stored as orc ;

CREATE TABLE if not exists student_parquet(name string,id tinyint,course string,year smallint)
stored as parquet ;

insert into table student_textfile select * from student ;

insert into table student_sequencefile select * from student ;

insert into table student_avro select * from student ;

insert into table student_rcfile select * from student ;

insert into table student_orc select * from student ;

insert into table student_parquet select * from student ;


create table if not exists docs(line string)
row format delimited 
;


select s.* from 
(select * from sales ) s 
left outer join products p 
on s.name=p.name 
where p.name is NULL
;

--Incremental load 
select t1.* from 
(select * from tablename) t1 
join
(select id,max(hiredate) as maxdate from 
(select * from tablename) t2
group by id ) s 
on t1.id=s.id and t1.date=s.maxdate 
;

select * from t1.* from 
(select * from table1) t1
join
(select id,max(hiredate) from 
(select * from table1) t2
group by id )s
on t1.id=s.id and t1.hiredate=s.maxdate 
;


select t1.* from 
(select * from table1) t1 
left outer join table2 t2
on t1.name=t2.name 
where t2.name is NULL
;






beeline -u jdbc:hive2://localhost:10000/default -n orienit -d org.apache.hive.jdbc.HiveDriver

beeline -u jdbc:hive2://cxln2.c.thelab-240901.internal:10000/default -n miriyalathriveni8198 -d org.apache.hive.jdbc.HiveDriver

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
