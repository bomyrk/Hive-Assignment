-- after starting hive, we have to add the serder .jar file with below command
ADD JAR /home/hadoop/Documents/hive-serdes-1.0-SNAPSHOT.jar ;
Added [/home/hadoop/Documents/hive-serdes-1.0-SNAPSHOT.jar] to class path
Added resources: [/home/hadoop/Documents/hive-serdes-1.0-SNAPSHOT.jar]

-- this the code to create the directory on hdfs
--hdfs dfs -mkdir json_data

-- this is to copy from local disk to hdfs
--hdfs dfs -copyFromLocal Data/* json_data 

-- this is to check the data is present
--hdfs dfs -ls json_data

-- create the data base

CREATE DATABASE `json_data`
COMMENT 'hive database json data'
LOCATION
  'hdfs://localhost:9000/user/hive/warehouse/json_data.db'
WITH DBPROPERTIES ('creator'='Bomyr Kamguia', 'date'='2023-05-24');

-- create the TABLE
CREATE TABLE json_table(json STRING);

-- load the data into the TABLE

LOAD DATA INPATH '/user/hadoop/json_data/' OVERWRITE INTO TABLE json_table;

-- get the text of the tweetContent
select get_json_object(json,'$.text') as tweetContent from json_table;

/* RT @JatinKiDuniya: My budget is 27k &amp; I want to buy Mobile, Harddisk, backpack, shoes &amp; watch which ones should i buy from @flipkart? #Flip…
RT @PDM9: Trump vs. Clinton. Welke presidentskandidaat heeft de beste online marketing aanpak? https://t.co/9IF2nKET72 by @bloovi
RT @lady_gabbar: So, the #Flipkartfreedomsale is on &amp; I need some help. If you help me shop, I'll give 5 of you flipkart vouchers worth Rs.…
#Sandisk #Cruzer Blade 16 GB #Utility #Pendrive Flat 35% Off https://t.co/xNE5LsHqww https://t.co/nZZeWlmezL
@JatinKiDuniya you should buy all these things in your budget RS. 27000 
@Flipkart #FlipkartFreedomSale https://t.co/YQ3EtKk2DS
@JatinKiDuniya @Flipkart go for Lenovo k5 note and maxima watch, woodland shoes, HD n skybags  #FlipkartFreedomSale https://t.co/7rfP3CDeoW
VACATURE: STAGIAIR VIDEOMARKETING. Heb jij een passie voor video en online marketing? Kom dan ons marketing team versterken! #stage #video
@AyeNuMe @Flipkart #FlipkartFreedomSale https://t.co/8toGDqPMzU
RT @lady_gabbar: My budget is equal to my followers-count. Tell me which earphones, mobile and hardisk should I buy from @flipkart? #Flipka…
Social Media Marketing for picking up clients online (FULL TUTORIAL) Facebook Reddit YouTube https://t.co/fyiJHuloLA https://t.co/juwSw2qzNp
RT @bennchi2010: Einkommen -  https://t.co/BNAAKoiXw0 Der Online Marketing Club von Thomas Klußmann https://t.co/V2ShOaX3PD
Hey vivek i wanna suggest you....u have to  complaint in consumer form..@Flipkart @vsvivekgautam https://t.co/7am81VkWvf
RT @yunbitsoftware: TODO EN UNO
  Gestión de webs y tiendas online
  Marketing y promociones
  Finanzas
  Logística
  Analítica y más https…
The latest  Billyqb Online Advertising ! https://t.co/2p4Fu18ijO Thanks to @HeaIthyTlPS @FirebaughNorman @LaCapital_ #marketing #advertising
#CatchMyCoupon -  Flipkart - Laurels, HawaiShop... Minimum 40% Off: Flipkart - Laurels,… https://t.co/kd3bt1xZfF https://t.co/4Q6DjU4Y7a
#CatchMyCoupon -  Flipkart - iPro Just at Rs.699: Flipkart - iPro Just at Rs.699Offer… https://t.co/rbPp2WsU5m https://t.co/YP3p9Rs29a
RT @HITKPKD: @priyalpoddar That’s a cool way of killing mosquitoes!. Keep sharing more ways to win Flipkart vouchers! #HitToKill https://t.…
#CatchMyCoupon -  Flipkart - SanDisk Just at Rs.249: Flipkart - SanDisk Just at Rs.249Offer… https://t.co/vejZn6DnfN https://t.co/eeM7WXxtQG
#CatchMyCoupon -  Flipkart - Jeans, Shirts... Alan Jones, Duke...: Flipkart - Jeans,… https://t.co/vu5kv5MRjc https://t.co/4Dc7EVlCq0
Flipkart shares marked up 10% by Valic, co now valued at $11.6 bn https://t.co/gqZoKGZkkS
RT @JatinKiDuniya: My budget is 27k &amp; I want to buy Mobile, Harddisk, backpack, shoes &amp; watch which ones should i buy from @flipkart? #Flip…
@JatinKiDuniya @Flipkart #FlipkartFreedomSale These products in pics &amp; Adidas DURAMO 7 shoe 
https://t.co/WXmcqrijkE https://t.co/u6VaClLmDU
@AyeNuMe @Flipkart #FlipkartFreedomSale https://t.co/3eIuEWRRpF
The best powerbank at best price
@JatinKiDuniya @Flipkart i have suggested all!
Nd you cn buy even more wid #FlipkartFreedomSale 
They are great,
Best thing wid great price!
Flipkart valuation seen at $11.5 billion after Valic markup https://t.co/03u89PCKd1
...
Time taken: 0.203 seconds, Fetched: 703 row(s) */


-- get the user name
select get_json_object(json,'$.user.name') as user from json_table;

/* Silky Saraf
Ilse Himschoot
Nisha Kolay
shopmava
Gourav Verma
Your Love ❤️
Coney
Ruchi
Nisha Kolay
avkworld.com
Infothek
Agnivesh Yadav
la meva
Guillermo Quiroz
CatchMyCoupon
CatchMyCoupon
Chanchal
CatchMyCoupon
CatchMyCoupon
NewsWallet
snehalata jain
Mayank Kumar
Ruchi
Er. Rahul❗
John Mark Kandukuri
...
Time taken: 0.222 seconds, Fetched: 703 row(s) */

-- get the user id of the entity
select get_json_object(json, '$.id') as identifier from json_table;

/* 763652545684406272
763652595550392320
763652619520925696
763652639556894720
763652640999743488
763652650017628160
763652662378262528
763652665159086080
763652685837066240
763652707227799552
763652731169075201
763652784227028992
763652798374379520
763652803998908416
763652835162501120
763652841177088000
763652843211415552
763652846365442048
763652854993162240
763652863159664640
763652866385047552
763652872177344512
763652893962559488
763652897691275264
763653002263601152
Time taken: 0.196 seconds, Fetched: 703 row(s) */


-- get the source 
select get_json_object(json,'$.source') as screenName from json_table ;

/* <a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="https://roundteam.co" rel="nofollow">RoundTeam</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://publicize.wp.com/" rel="nofollow">WordPress.com</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>
<a href="http://bufferapp.com" rel="nofollow">Buffer</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://publicize.wp.com/" rel="nofollow">WordPress.com</a>
<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>
<a href="http://paper.li" rel="nofollow">Paper.li</a>
<a href="http://dlvr.it" rel="nofollow">dlvr.it</a>
<a href="http://dlvr.it" rel="nofollow">dlvr.it</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://dlvr.it" rel="nofollow">dlvr.it</a>
<a href="http://dlvr.it" rel="nofollow">dlvr.it</a>
<a href="http://newswallet.co/" rel="nofollow">NewsWallet</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>
<a href="http://www.linkedin.com/" rel="nofollow">LinkedIn</a>
...
Time taken: 0.195 seconds, Fetched: 703 row(s) */


--get the follower count
select get_json_object(json, '$.user.followers_count') as follower_count from json_table;

/* 12696
2380
2012
3
8702
6533
5372
1095
2012
70
1259
33
19
1327
0
0
6760
0
0
262
14373
2667
1095
4789
88
...
Time taken: 0.213 seconds, Fetched: 703 row(s) */


--get the friends count
select get_json_object(json, '$.user.friends_count') as friends_count from json_table ;

/* 2458
1953
2186
30
2512
208
4984
1751
2186
0
1615
45
86
2207
0
0
3647
0
0
30
3604
1870
1751
128
119
...
Time taken: 0.215 seconds, Fetched: 703 row(s) */


-- get the screen name
select get_json_object(json, '$.user.screen_name') as screen_name from json_table ;

/* saraf_silky
IlseHimschoot
Jhalli_lily
shopmavablog
GouravvRock
ModelGarima
ConeyDataDriven
rpunamia
Jhalli_lily
Avkworld
bennchi2010
agnidj507
deltarap2
billyqb
CatchMyCoupon
CatchMyCoupon
Sensible_Mona
CatchMyCoupon
CatchMyCoupon
newswalletapp
SnehalataJ
krmayank13
rpunamia
rahul_shaitan
jmkandukuri
...
Time taken: 0.207 seconds, Fetched: 703 row(s) */
