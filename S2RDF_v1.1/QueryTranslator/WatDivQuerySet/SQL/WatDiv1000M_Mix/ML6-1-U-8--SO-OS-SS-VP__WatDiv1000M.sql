SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User3579677'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__author$$3$$
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM wsdbm__makesPurchase$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__purchaseFor$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM sorg__contentRating$$6$$
	
	) tab5
 ON(tab4.v5=tab5.v5)


++++++Tables Statistic
sorg__author$$3$$	2	OS	sorg__author/wsdbm__makesPurchase
	VP	<sorg__author>	399974
	SO	<sorg__author><wsdbm__likes>	377768	0.94
	OS	<sorg__author><wsdbm__makesPurchase>	35583	0.09
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	449969341
	OS	<wsdbm__friendOf><wsdbm__likes>	107178903	0.24
------
sorg__contentRating$$6$$	1	SO	sorg__contentRating/wsdbm__purchaseFor
	VP	<sorg__contentRating>	750835
	SO	<sorg__contentRating><wsdbm__purchaseFor>	454154	0.6
------
wsdbm__likes$$2$$	2	OS	wsdbm__likes/sorg__author
	VP	<wsdbm__likes>	11246476
	SO	<wsdbm__likes><wsdbm__friendOf>	11246476	1.0
	OS	<wsdbm__likes><sorg__author>	1428651	0.13
------
wsdbm__makesPurchase$$4$$	1	SO	wsdbm__makesPurchase/sorg__author
	VP	<wsdbm__makesPurchase>	14999930
	SO	<wsdbm__makesPurchase><sorg__author>	327172	0.02
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	14999930	1.0
------
wsdbm__purchaseFor$$5$$	2	OS	wsdbm__purchaseFor/sorg__contentRating
	VP	<wsdbm__purchaseFor>	15000000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	14999930	1.0
	OS	<wsdbm__purchaseFor><sorg__contentRating>	4048866	0.27
------
