SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User64972'
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
	VP	<sorg__author>	3975
	SO	<sorg__author><wsdbm__likes>	3786	0.95
	OS	<sorg__author><wsdbm__makesPurchase>	341	0.09
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	4491142
	OS	<wsdbm__friendOf><wsdbm__likes>	1074144	0.24
------
sorg__contentRating$$6$$	1	SO	sorg__contentRating/wsdbm__purchaseFor
	VP	<sorg__contentRating>	7530
	SO	<sorg__contentRating><wsdbm__purchaseFor>	5394	0.72
------
wsdbm__likes$$2$$	2	OS	wsdbm__likes/sorg__author
	VP	<wsdbm__likes>	112401
	SO	<wsdbm__likes><wsdbm__friendOf>	112401	1.0
	OS	<wsdbm__likes><sorg__author>	11978	0.11
------
wsdbm__makesPurchase$$4$$	1	SO	wsdbm__makesPurchase/sorg__author
	VP	<wsdbm__makesPurchase>	149998
	SO	<wsdbm__makesPurchase><sorg__author>	3135	0.02
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	149998	1.0
------
wsdbm__purchaseFor$$5$$	2	OS	wsdbm__purchaseFor/sorg__contentRating
	VP	<wsdbm__purchaseFor>	150000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	149998	1.0
	OS	<wsdbm__purchaseFor><sorg__contentRating>	32864	0.22
------
