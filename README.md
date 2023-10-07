# Hive Assignment
 Hive Assignment with usage of JSON SerDe


### Assignment 1
Using Hive JSON Serde, you have to extract the following fields
 - data link: https://intellipaat-course-attachments.s3.ap-south-1.amazonaws.com/Hadoop/hadoop_dataset.rar

After starting hive, we have to add the serder .jar file with the below command
 - `ADD JAR /home/hadoop/Documents/hive-serdes-1.0-SNAPSHOT.jar ;`
 - `Added [/home/hadoop/Documents/hive-serdes-1.0-SNAPSHOT.jar] to class path`
 - `Added resources: [/home/hadoop/Documents/hive-serdes-1.0-SNAPSHOT.jar]`

-- This is the code to create the directory on hdfs
`hdfs dfs -mkdir json_data`

-- this is to copy from local disk to hdfs
`hdfs dfs -copyFromLocal Data/* json_data `

-- this is to check the data is present
`hdfs dfs -ls json_data`

-- create the data base

```
CREATE DATABASE `json_data`
COMMENT 'hive database json data'
LOCATION
  'hdfs://localhost:9000/user/hive/warehouse/json_data.db'
WITH DBPROPERTIES ('creator'='Bomyr Kamguia', 'date'='2023-05-24');
```

-- create the TABLE
`
CREATE TABLE json_table(json STRING);
`

-- load the data into the TABLE

`
LOAD DATA INPATH '/user/hadoop/json_data/' OVERWRITE INTO TABLE json_table;
`

-- get the text of the tweetContent

`
select get_json_object(json,'$.text') as tweetContent from json_table;
`

-- get the user name

`select get_json_object(json,'$.user.name') as user from json_table;`

-- get the user id of the entity

`select get_json_object(json, '$.id') as identifier from json_table;`

-- get the source 

`select get_json_object(json,'$.source') as screenName from json_table ;`

-- get the follower count

`select get_json_object(json, '$.user.followers_count') as follower_count from json_table;`

-- get the friends count

`select get_json_object(json, '$.user.friends_count') as friends_count from json_table ;`

-- get the screen name

`select get_json_object(json, '$.user.screen_name') as screen_name from json_table ;`

### Assignment 2
```
-- CREATE DATABASE;
CREATE DATABASE `facebook`
LOCATION
  'hdfs://localhost:9000/user/hive/warehouse/facebook.db'

-- CREATE Table to store data with the rigth schema
CREATE TABLE `facebooknew`(
  `userid` int, 
  `age` int, 
  `dob_day` int, 
  `dob_year` int, 
  `dob_month` int, 
  `gender` string, 
  `tenure` int, 
  `friends_count` int, 
  `friends_ini` int, 
  `likes` int, 
  `likes_recd` int, 
  `mlikes` int, 
  `mlikes_recd` int, 
  `weblikes` int, 
  `weblikes_recd` int)
COMMENT  'facebook data'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
--Check the TABLE

DESCRIBE FORMATTED facebooknew;

-- as data has first row with columns names, tell to the engine to skip it

ALTER TABLE facebooknew SET TBLPROPERTIES ("skip.header.line.count"="1");

-- hadoop code to load data into HDFS
--hdfs dfs -put FacebookData.csv fbdata/

-- Load data into the table just created 
LOAD DATA INPATH '/user/hadoop/fbdata/FacebookData.csv' OVERWRITE INTO TABLE facebooknew;

-- look at characteristic of TABLESPACE
SHOW TABLE EXTENDED LIKE "face.*";

--check the number of row
SELECT count(*) FROM facebooknew;
--99003

```
1. What is the average age of a social media account user? `SELECT AVG(age) FROM facebooknew;`
2. Does the social media platform have a higher number of male users, female users, or gender undisclosed users? `SELECT gender, count(*) FROM facebooknew GROUP BY gender;`
3. In male users, on average, does the age demographic of 13–25 have more, or less, friends than the demographic of 26–50? Assess this with appropriate statistical reasoning
```
SELECT gender, avg(friends_count) AS tran_1 FROM facebooknew 
WHERE age >=13 and age <=25
GROUP BY gender;

SELECT gender, avg(friends_count) As tran_2 FROM facebooknew 
WHERE age >=26 and age <=50
GROUP BY gender;

```
4. In female users, on average, does the age demographic of 13–25 have more, or less, friends than the demographic of 26–50? Assess this with appropriate statistical reasoning
```
SELECT t1.gender, t1.tran_1, t2.tran_2
from (SELECT gender, avg(friends_count) AS tran_1 FROM facebooknew 
WHERE age >=13 and age <=25
GROUP BY gender) as t1, (SELECT gender, avg(friends_count) As tran_2 FROM facebooknew 
WHERE age >=26 and age <=50
GROUP BY gender) as t2
WHERE t1.gender = t2.gender;
```
5. Which gender is more likely to send out a higher number of friend requests on average? `SELECT gender, avg(friends_ini) FROM facebooknew GROUP BY gender  ;`
6. With the age demographics of 13–25, 26–35, and 36–50 as focal points, evaluate the comparison between mobile application usage and web browser usage when accessing the social media website. Use this to determine if mobile phones have indeed taken over the digital marketspace
```
SELECT round(avg(mlikes)) AS ml_avg, round(avg(mlikes_recd)) AS mlr_avg, round(avg(mlikes + mlikes_recd)) as mlt_avg,
round(avg(weblikes)) AS webl_avg, round(avg(weblikes_recd)) AS weblr_avg, round(avg(weblikes + weblikes_recd)) as weblt_avg
 FROM facebooknew 
WHERE age >=13 and age <=25 
union all
SELECT round(avg(mlikes)) AS ml_avg, round(avg(mlikes_recd)) AS mlr_avg, round(avg(mlikes + mlikes_recd)) as mlt_avg,
round(avg(weblikes)) AS webl_avg, round(avg(weblikes_recd)) AS weblr_avg, round(avg(weblikes + weblikes_recd)) as weblt_avg
 FROM facebooknew 
WHERE age >=26 and age <=35 
union all
SELECT round(avg(mlikes)) AS ml_avg, round(avg(mlikes_recd)) AS mlr_avg, round(avg(mlikes + mlikes_recd)) as mlt_avg,
round(avg(weblikes)) AS webl_avg, round(avg(weblikes_recd)) AS weblr_avg, round(avg(weblikes + weblikes_recd)) as weblt_avg
 FROM facebooknew 
WHERE age >=36 and age <=50 ;

```

```
SELECT avg(mlikes) AS ml_avg, avg(mlikes_recd) AS mlr_avg, avg(mlikes + mlikes_recd) as mlt_avg,
avg(weblikes) AS webl_avg, avg(weblikes_recd) AS weblr_avg, avg(weblikes + weblikes_recd) as weblt_avg
 FROM facebooknew 
WHERE age >=13 and age <=25 ;
-->123.98981737425284	119.8740283979493	243.86384577220215	55.50010631511801	80.41295154393177	135.9130578590498

SELECT avg(mlikes) AS ml_avg, avg(mlikes_recd) AS mlr_avg, avg(mlikes + mlikes_recd) as mlt_avg,
avg(weblikes) AS webl_avg, avg(weblikes_recd) AS weblr_avg, avg(weblikes + weblikes_recd) as weblt_avg
 FROM facebooknew 
WHERE age >=26 and age <=35 ;
-->88.91646547561564	56.809807393100485	145.72627286871614	21.87735393529696	32.6137668329846	54.49112076828156

SELECT avg(mlikes) AS ml_avg, avg(mlikes_recd) AS mlr_avg, avg(mlikes + mlikes_recd) as mlt_avg,
avg(weblikes) AS webl_avg, avg(weblikes_recd) AS weblr_avg, avg(weblikes + weblikes_recd) as weblt_avg
 FROM facebooknew 
WHERE age >=36 and age <=50 ;
-->103.65690015117703	60.71326758332734	164.37016773450435	37.47368799942409	44.985026276006046	82.45871427543014

SELECT round(avg(mlikes)) AS ml_avg, round(avg(mlikes_recd)) AS mlr_avg, round(avg(mlikes + mlikes_recd)) as mlt_avg,
round(avg(weblikes)) AS webl_avg, round(avg(weblikes_recd)) AS weblr_avg, round(avg(weblikes + weblikes_recd)) as weblt_avg
FROM facebooknew 
WHERE age >=36 and age <=50 ;
```

### Assignment 3

```
-- CREATE DATABASE;
CREATE DATABASE `Demo`
LOCATION
  'hdfs://localhost:9000/user/hive/warehouse/demo.db'
COMMENT 'hive database suicide'
WITH DBPROPERTIES ('creator'='Bomyr Kamguia', 'date'='2023-05-23');

-- CREATE Table to store data with the rigth schema
--State,Year,Type_code,Type,Gender,Age_group,Total
CREATE EXTERNAL TABLE `Suicides`(
  `state` string, 
  `year` int, 
  `type_code` string,  
  `type` string, 
  `gender` string, 
  `age_group` string,  
  `total` int)
COMMENT  'suicides data'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

--Check the TABLE

DESCRIBE FORMATTED Suicides;

-- as data has first row with columns names, tell to the engine to skip it

ALTER TABLE Suicides SET TBLPROPERTIES ("skip.header.line.count"="1");

-- hadoop code to load data into HDFS
--hdfs dfs -put Suicides.csv input/

-- Load data into the table just created 
LOAD DATA INPATH '/user/hadoop/input/Suicides.csv' OVERWRITE INTO TABLE Suicides;

-- look at characteristic of TABLESPACE
SHOW TABLE EXTENDED LIKE "Suic.*";

--check the number of row
SELECT count(*) FROM Suicides;
```

You can continue by yourself or look at answers in fil attached

