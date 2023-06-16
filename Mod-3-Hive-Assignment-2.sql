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
>99003

--1.What is the average age of a social media account user?
SELECT AVG(age) FROM facebooknew;
> 37.28022383160106

--2. Does the social media platform have a higher number of male users, female users, 
--or gender undisclosed users?

--R. Male use social media more than female
SELECT gender, count(*) FROM facebooknew GROUP BY gender;
>NA		175
>female	40254
>male	58574

--3. In male users, on average, does the age demographic of 13–25 have more, or less, 
--friends than the demographic of 26–50? Assess this with appropriate statistical reasoning
--4. In female users, on average, does the age demographic of 13–25 have more, or less, 
--friends than the demographic of 26–50? Assess this with appropriate statistical reasoning

--R. On average female has more friends in 13-25 yrs than in [26-50 yrs  on social media
--R. On average male has more friends in 26-50 yrs than in [26-50 yrs  on social media
SELECT gender, avg(friends_count) AS tran_1 FROM facebooknew 
WHERE age >=13 and age <=25
GROUP BY gender;

NA	112.84615384615384
female	380.8462854969574
male	201.87256010249453

SELECT gender, avg(friends_count) As tran_2 FROM facebooknew 
WHERE age >=26 and age <=50
GROUP BY gender;

-->NA	176.14285714285714
-->female	134.46947862694302
-->male	101.1409938504265

SELECT t1.gender, t1.tran_1, t2.tran_2
from (SELECT gender, avg(friends_count) AS tran_1 FROM facebooknew 
WHERE age >=13 and age <=25
GROUP BY gender) as t1, (SELECT gender, avg(friends_count) As tran_2 FROM facebooknew 
WHERE age >=26 and age <=50
GROUP BY gender) as t2
WHERE t1.gender = t2.gender;

-->gender	tran_1 [13-25]		tran_2 [26-50]
-->NA		112.84615384615384	176.14285714285714
-->female	380.8462854969574	134.46947862694302
-->male	201.87256010249453	101.1409938504265




--5. Which gender is more likely to send out a higher number of friend requests on average?

--R. female


SELECT gender, avg(friends_ini) FROM facebooknew GROUP BY gender  ;

-->NA		92.57142857142857
-->female	113.89909077358772
-->male	103.06659951514324

--6. With the age demographics of 13–25, 26–35, and 36–50 as focal points, evaluate 
--the comparison between mobile application usage and web browser usage when accessing 
--the social media website. Use this to determine if mobile phones have indeed 
--taken over the digital marketspace

--R. Whatever the age they are more likely comfortable to use mobile phone than web platform
--R. however when it concer like received they received more likes than they give on web platform

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

-->  Age			ml_avg				mlr_avg					mlt_avg			webl_avg			weblr_avg					weblt_avg
--> [13-25] 123.98981737425284	119.8740283979493		243.86384577220215	55.50010631511801	80.41295154393177	135.9130578590498
--> [26-35] 88.91646547561564		56.809807393100485		145.72627286871614	21.87735393529696	32.6137668329846	54.49112076828156
--> [36-50] 103.65690015117703	60.71326758332734		164.37016773450435	37.47368799942409	44.985026276006046	82.45871427543014

--Result with ROUND
-->Age	ml_avg	mlr_avg	mlt_avg	webl_avg weblr_avg	weblt_avg
-->[13-25] 124.0	120.0	244.0	56.0	80.0	136.0
-->[25-35] 89.0	57.0	146.0	22.0	33.0	54.0
-->[36-50] 104.0	61.0	164.0	37.0	45.0	82.0



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