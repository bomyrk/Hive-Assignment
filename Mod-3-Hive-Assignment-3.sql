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
>237519

--4. Find out the most common suicide cause among females in India over the entire period 2001–2012

select type, sum(total) as nbr
from Suicides
where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes' and gender = 'Female' and year >= 2001 and year <= 2012 
group by  type
order by  nbr desc
limit 1;

>Family Problems	133181


--5. Find out the state-wise most common cause among males over the entire period

-- state wise most number of suicides
select t1.state, max(t1.nbr)
from
(select state, type, sum(total) as nbr
from Suicides
where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes' and gender = 'Male'
group by state, type
order by state asc, nbr desc) t1
group by t1.state;


-- state wise number suicides per type of cause
select state, type, sum(total) as nbr
from Suicides
where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes' and gender ='Male'
group by state, type
order by state asc, nbr desc;

-- state wise most common cause among males 
select t3.state, t3.mx, t2.type
from
 (select t1.state, max(t1.nbr) as mx
 from
   (select state, type,  sum(total) as nbr
    from Suicides
    where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes' and gender ='Male'
    group by state, type
    order by state asc, nbr desc) t1
  group by t1.state ) t3
left join 
  (select state, type, sum(total) as nbr
   from Suicides
   where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes' and gender ='Male'
   group by state, type
   order by state asc, nbr desc) t2 
where t3.state = t2.state and t3.mx = t2.nbr;
/* 
A & N Islands	296	Other Causes (Please Specity)
Andhra Pradesh	20795	Other Prolonged Illness
Arunachal Pradesh	556	Causes Not known
Assam	6883	Causes Not known
Bihar	1847	Other Causes (Please Specity)
Chandigarh	161	Family Problems
Chhattisgarh	13123	Causes Not known
D & N Haveli	83	Family Problems
Daman & Diu	97	Causes Not known
Delhi (Ut)	3857	Causes Not known
Goa	602	Causes Not known
Gujarat	8351	Causes Not known
Haryana	8127	Other Causes (Please Specity)
Himachal Pradesh	1085	Other Causes (Please Specity)
Jammu & Kashmir	461	Causes Not known
Jharkhand	2429	Other Causes (Please Specity)
Karnataka	23833	Causes Not known
Kerala	26558	Family Problems
Lakshadweep	2	Other Prolonged Illness
Madhya Pradesh	13334	Causes Not known
Maharashtra	39208	Family Problems
Manipur	198	Causes Not known
Meghalaya	414	Causes Not known
Mizoram	356	Causes Not known
Nagaland	142	Causes Not known
Odisha	8767	Causes Not known
Puducherry	1851	Family Problems
Punjab	1792	Insanity/Mental Illness
Rajasthan	10437	Other Causes (Please Specity)
Sikkim	448	Causes Not known
Tamil Nadu	29761	Family Problems
Tripura	1586	Family Problems
Uttar Pradesh	6364	Causes Not known
Uttarakhand	599	Causes Not known
West Bengal	23278	Causes Not known
Time taken: 108.639 seconds, Fetched: 35 row(s) */


--6. Find out the age group-wise most common cause among males and females

-- age-group wise most number of suicides
select t1.age_group, max(t1.nbr)
from
(select age_group, type,  sum(total) as nbr
from Suicides
where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes'
group by age_group, type
order by age_group asc, nbr desc) t1
group by t1.age_group;

-- age-group wise number suicides per cype of cause
select age_group, type, sum(total) as nbr
from Suicides
where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes'
group by age_group, type
order by age_group asc, nbr desc;

-- age group wise most common cause among males and females (together)
select t3.age_group, t3.mx, t2.type
from
 (select t1.age_group, max(t1.nbr) as mx
 from
   (select age_group, type,  sum(total) as nbr
    from Suicides
    where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes'
    group by age_group, type
    order by age_group asc, nbr desc) t1
  group by t1.age_group ) t3
left join 
  (select age_group, type, sum(total) as nbr
   from Suicides
   where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes'
   group by age_group, type
   order by age_group asc, nbr desc) t2 
where t3.age_group = t2.age_group and t3.mx = t2.nbr


-- age group wise most common cause among males and females (separetly)
select t3.gender, t3.age_group, t3.mx, t2.type
from
 (select t1.gender, t1.age_group, max(t1.nbr) as mx
 from
   (select gender, age_group, type,  sum(total) as nbr
    from Suicides
    where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes'
    group by gender, age_group, type
    order by gender desc, age_group asc, nbr desc) t1
  group by t1.gender, t1.age_group ) t3
left join 
  (select gender, age_group, type, sum(total) as nbr
   from Suicides
   where state not in ('Total (All India)', 'Total (States)', 'Total (Uts)') and type_code = 'Causes'
   group by gender, age_group, type
   order by gender desc, age_group asc, nbr desc) t2 
where t3.gender = t2.gender and t3.age_group = t2.age_group and t3.mx = t2.nbr;


/* Female	0-14	4082	Other Causes (Please Specity)
Female	15-29	60299	Family Problems
Female	30-44	44238	Family Problems
Female	45-59	19924	Family Problems
Female	60+	9369	Other Prolonged Illness
Male	0-14	4953	Other Causes (Please Specity)
Male	15-29	61128	Family Problems
Male	30-44	80897	Family Problems
Male	45-59	49541	Family Problems
Male	60+	21713	Other Prolonged Illness
Time taken: 108.908 seconds, Fetched: 10 row(s) */



--7. Find out the total number of suicides per year per state

select year, state, sum(total)
from Suicides
where state not in ('Total (All India)', 'Total (States)') and type_code = 'Causes'
group by year, state
order by year, state;

/* 2001	A & N Islands	129
2001	Andhra Pradesh	10522
2001	Arunachal Pradesh	111
2001	Assam	2647
2001	Bihar	603
2001	Chandigarh	70
2001	Chhattisgarh	4025
2001	D & N Haveli	50
2001	Daman & Diu	14
2001	Delhi (Ut)	1239
2001	Goa	256
2001	Gujarat	4791
2001	Haryana	2007
2001	Himachal Pradesh	307
2001	Jammu & Kashmir	153
2001	Jharkhand	250
2001	Karnataka	11881
2001	Kerala	9572
2001	Lakshadweep	0
2001	Madhya Pradesh	6860
2001	Maharashtra	14618
2001	Manipur	41
2001	Meghalaya	87
2001	Mizoram	54
2001	Nagaland	40
2001	Odisha	4052
2001	Puducherry	529
2001	Punjab	648
2001	Rajasthan	3195
2001	Sikkim	94
2001	Tamil Nadu	11290
2001	Tripura	854
2001	Uttar Pradesh	3516
2001	Uttarakhand	311
2001	West Bengal	13690
2002	A & N Islands	144
2002	Andhra Pradesh	11693
2002	Arunachal Pradesh	114
2002	Assam	2510
2002	Bihar	720
2002	Chandigarh	87
2002	Chhattisgarh	3950
2002	D & N Haveli	50
2002	Daman & Diu	17
2002	Delhi (Ut)	1053
2002	Goa	309
2002	Gujarat	4644
2002	Haryana	2200
2002	Himachal Pradesh	334
2002	Jammu & Kashmir	184
2002	Jharkhand	272
2002	Karnataka	12270
2002	Kerala	9810
2002	Lakshadweep	0
2002	Madhya Pradesh	6899
2002	Maharashtra	14529
2002	Manipur	39
2002	Meghalaya	67
2002	Mizoram	66
2002	Nagaland	27
2002	Odisha	4388
2002	Puducherry	567
2002	Punjab	507
2002	Rajasthan	3248
2002	Sikkim	78
2002	Tamil Nadu	11244
2002	Tripura	779
2002	Uttar Pradesh	4250
2002	Uttarakhand	361
2002	West Bengal	13007
2003	A & N Islands	113
2003	Andhra Pradesh	11409
2003	Arunachal Pradesh	81
2003	Assam	2596
2003	Bihar	599
2003	Chandigarh	103
2003	Chhattisgarh	3919
2003	D & N Haveli	52
2003	Daman & Diu	24
2003	Delhi (Ut)	1153
2003	Goa	300
2003	Gujarat	4566
2003	Haryana	2227
2003	Himachal Pradesh	386
2003	Jammu & Kashmir	138
2003	Jharkhand	272
2003	Karnataka	12361
2003	Kerala	9438
2003	Lakshadweep	2
2003	Madhya Pradesh	6762
2003	Maharashtra	14760
2003	Manipur	26
2003	Meghalaya	41
2003	Mizoram	52
2003	Nagaland	22
2003	Odisha	4420
2003	Puducherry	582
2003	Punjab	631
2003	Rajasthan	3661
2003	Sikkim	105
2003	Tamil Nadu	11872
2003	Tripura	844
2003	Uttar Pradesh	3663
2003	Uttarakhand	391
2003	West Bengal	13280
2004	A & N Islands	122
2004	Andhra Pradesh	13526
2004	Arunachal Pradesh	79
2004	Assam	2839
2004	Bihar	351
2004	Chandigarh	75
2004	Chhattisgarh	4495
2004	D & N Haveli	39
2004	Daman & Diu	13
2004	Delhi (Ut)	1256
2004	Goa	314
2004	Gujarat	4776
2004	Haryana	2082
2004	Himachal Pradesh	371
2004	Jammu & Kashmir	112
2004	Jharkhand	417
2004	Karnataka	11937
2004	Kerala	9053
2004	Lakshadweep	0
2004	Madhya Pradesh	6795
2004	Maharashtra	14729
2004	Manipur	41
2004	Meghalaya	55
2004	Mizoram	60
2004	Nagaland	31
2004	Odisha	4215
2004	Puducherry	539
2004	Punjab	645
2004	Rajasthan	3725
2004	Sikkim	98
2004	Tamil Nadu	12839
2004	Tripura	770
2004	Uttar Pradesh	3637
2004	Uttarakhand	237
2004	West Bengal	13424
2005	A & N Islands	139
2005	Andhra Pradesh	13442
2005	Arunachal Pradesh	70
2005	Assam	2846
2005	Bihar	543
2005	Chandigarh	89
2005	Chhattisgarh	4881
2005	D & N Haveli	69
2005	Daman & Diu	32
2005	Delhi (Ut)	1245
2005	Goa	282
2005	Gujarat	4765
2005	Haryana	2046
2005	Himachal Pradesh	359
2005	Jammu & Kashmir	294
2005	Jharkhand	808
2005	Karnataka	11557
2005	Kerala	9244
2005	Lakshadweep	0
2005	Madhya Pradesh	5448
2005	Maharashtra	14426
2005	Manipur	27
2005	Meghalaya	71
2005	Mizoram	55
2005	Nagaland	27
2005	Odisha	4208
2005	Puducherry	538
2005	Punjab	588
2005	Rajasthan	4178
2005	Sikkim	109
2005	Tamil Nadu	12076
2005	Tripura	715
2005	Uttar Pradesh	3449
2005	Uttarakhand	273
2005	West Bengal	15015
2006	A & N Islands	133
2006	Andhra Pradesh	13276
2006	Arunachal Pradesh	129
2006	Assam	3031
2006	Bihar	618
2006	Chandigarh	80
2006	Chhattisgarh	4626
2006	D & N Haveli	42
2006	Daman & Diu	22
2006	Delhi (Ut)	1492
2006	Goa	275
2006	Gujarat	5035
2006	Haryana	2316
2006	Himachal Pradesh	457
2006	Jammu & Kashmir	262
2006	Jharkhand	856
2006	Karnataka	12212
2006	Kerala	9026
2006	Lakshadweep	2
2006	Madhya Pradesh	6435
2006	Maharashtra	15494
2006	Manipur	36
2006	Meghalaya	92
2006	Mizoram	70
2006	Nagaland	28
2006	Odisha	4065
2006	Puducherry	526
2006	Punjab	772
2006	Rajasthan	4263
2006	Sikkim	145
2006	Tamil Nadu	12381
2006	Tripura	765
2006	Uttar Pradesh	3099
2006	Uttarakhand	326
2006	West Bengal	15725
2007	A & N Islands	156
2007	Andhra Pradesh	14882
2007	Arunachal Pradesh	129
2007	Assam	3062
2007	Bihar	965
2007	Chandigarh	82
2007	Chhattisgarh	4839
2007	D & N Haveli	76
2007	Daman & Diu	15
2007	Delhi (Ut)	1481
2007	Goa	270
2007	Gujarat	5580
2007	Haryana	2433
2007	Himachal Pradesh	402
2007	Jammu & Kashmir	234
2007	Jharkhand	1289
2007	Karnataka	12304
2007	Kerala	8962
2007	Lakshadweep	3
2007	Madhya Pradesh	6329
2007	Maharashtra	15184
2007	Manipur	39
2007	Meghalaya	87
2007	Mizoram	28
2007	Nagaland	24
2007	Odisha	4308
2007	Puducherry	517
2007	Punjab	847
2007	Rajasthan	4437
2007	Sikkim	122
2007	Tamil Nadu	13811
2007	Tripura	705
2007	Uttar Pradesh	3927
2007	Uttarakhand	248
2007	West Bengal	14860
2008	A & N Islands	143
2008	Andhra Pradesh	14354
2008	Arunachal Pradesh	110
2008	Assam	2989
2008	Bihar	1015
2008	Chandigarh	83
2008	Chhattisgarh	4945
2008	D & N Haveli	60
2008	Daman & Diu	19
2008	Delhi (Ut)	1303
2008	Goa	287
2008	Gujarat	6165
2008	Haryana	2656
2008	Himachal Pradesh	630
2008	Jammu & Kashmir	310
2008	Jharkhand	911
2008	Karnataka	12222
2008	Kerala	8569
2008	Lakshadweep	0
2008	Madhya Pradesh	7629
2008	Maharashtra	14374
2008	Manipur	34
2008	Meghalaya	85
2008	Mizoram	41
2008	Nagaland	42
2008	Odisha	4904
2008	Puducherry	507
2008	Punjab	869
2008	Rajasthan	5166
2008	Sikkim	287
2008	Tamil Nadu	14425
2008	Tripura	752
2008	Uttar Pradesh	4088
2008	Uttarakhand	191
2008	West Bengal	14852
2009	A & N Islands	131
2009	Andhra Pradesh	14500
2009	Arunachal Pradesh	110
2009	Assam	2966
2009	Bihar	1051
2009	Chandigarh	75
2009	Chhattisgarh	5883
2009	D & N Haveli	56
2009	Daman & Diu	23
2009	Delhi (Ut)	1477
2009	Goa	278
2009	Gujarat	6156
2009	Haryana	2503
2009	Himachal Pradesh	560
2009	Jammu & Kashmir	321
2009	Jharkhand	1112
2009	Karnataka	12195
2009	Kerala	8755
2009	Lakshadweep	1
2009	Madhya Pradesh	9113
2009	Maharashtra	14300
2009	Manipur	27
2009	Meghalaya	112
2009	Mizoram	69
2009	Nagaland	31
2009	Odisha	4365
2009	Puducherry	518
2009	Punjab	847
2009	Rajasthan	5065
2009	Sikkim	241
2009	Tamil Nadu	14424
2009	Tripura	738
2009	Uttar Pradesh	4158
2009	Uttarakhand	342
2009	West Bengal	14648
2010	A & N Islands	156
2010	Andhra Pradesh	15901
2010	Arunachal Pradesh	131
2010	Assam	2993
2010	Bihar	1226
2010	Chandigarh	71
2010	Chhattisgarh	6522
2010	D & N Haveli	63
2010	Daman & Diu	31
2010	Delhi (Ut)	1543
2010	Goa	322
2010	Gujarat	6207
2010	Haryana	2895
2010	Himachal Pradesh	542
2010	Jammu & Kashmir	259
2010	Jharkhand	1232
2010	Karnataka	12651
2010	Kerala	8586
2010	Lakshadweep	1
2010	Madhya Pradesh	9003
2010	Maharashtra	15916
2010	Manipur	37
2010	Meghalaya	108
2010	Mizoram	76
2010	Nagaland	12
2010	Odisha	4255
2010	Puducherry	508
2010	Punjab	920
2010	Rajasthan	4920
2010	Sikkim	280
2010	Tamil Nadu	16561
2010	Tripura	725
2010	Uttar Pradesh	3628
2010	Uttarakhand	281
2010	West Bengal	16037
2011	A & N Islands	136
2011	Andhra Pradesh	15077
2011	Arunachal Pradesh	134
2011	Assam	2726
2011	Bihar	795
2011	Chandigarh	105
2011	Chhattisgarh	6756
2011	D & N Haveli	63
2011	Daman & Diu	33
2011	Delhi (Ut)	1716
2011	Goa	293
2011	Gujarat	6382
2011	Haryana	3245
2011	Himachal Pradesh	443
2011	Jammu & Kashmir	287
2011	Jharkhand	1212
2011	Karnataka	12622
2011	Kerala	8431
2011	Lakshadweep	0
2011	Madhya Pradesh	9259
2011	Maharashtra	15947
2011	Manipur	33
2011	Meghalaya	153
2011	Mizoram	90
2011	Nagaland	33
2011	Odisha	5241
2011	Puducherry	557
2011	Punjab	966
2011	Rajasthan	4348
2011	Sikkim	184
2011	Tamil Nadu	15963
2011	Tripura	703
2011	Uttar Pradesh	4843
2011	Uttarakhand	317
2011	West Bengal	16492
2012	A & N Islands	121
2012	Andhra Pradesh	14238
2012	Arunachal Pradesh	130
2012	Assam	3264
2012	Bihar	759
2012	Chandigarh	114
2012	Chhattisgarh	5654
2012	D & N Haveli	66
2012	Daman & Diu	36
2012	Delhi (Ut)	1899
2012	Goa	289
2012	Gujarat	7110
2012	Haryana	2827
2012	Himachal Pradesh	528
2012	Jammu & Kashmir	414
2012	Jharkhand	1319
2012	Karnataka	12753
2012	Kerala	8490
2012	Lakshadweep	1
2012	Madhya Pradesh	9775
2012	Maharashtra	16112
2012	Manipur	41
2012	Meghalaya	128
2012	Mizoram	173
2012	Nagaland	30
2012	Odisha	5027
2012	Puducherry	541
2012	Punjab	1030
2012	Rajasthan	4821
2012	Sikkim	181
2012	Tamil Nadu	16927
2012	Tripura	844
2012	Uttar Pradesh	4422
2012	Uttarakhand	424
2012	West Bengal	0
Time taken: 52.552 seconds, Fetched: 420 row(s)
 */
