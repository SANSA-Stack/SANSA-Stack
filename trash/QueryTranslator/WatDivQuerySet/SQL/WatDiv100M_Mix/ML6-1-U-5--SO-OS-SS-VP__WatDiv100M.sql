SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User450652'
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
	VP	<sorg__author>	40060
	SO	<sorg__author><wsdbm__likes>	38009	0.95
	OS	<sorg__author><wsdbm__makesPurchase>	3566	0.09
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	45092208
	OS	<wsdbm__friendOf><wsdbm__likes>	10736619	0.24
------
sorg__contentRating$$6$$	1	SO	sorg__contentRating/wsdbm__purchaseFor
	VP	<sorg__contentRating>	75412
	SO	<sorg__contentRating><wsdbm__purchaseFor>	49206	0.65
------
wsdbm__likes$$2$$	2	OS	wsdbm__likes/sorg__author
	VP	<wsdbm__likes>	1124672
	SO	<wsdbm__likes><wsdbm__friendOf>	1124672	1.0
	OS	<wsdbm__likes><sorg__author>	105725	0.09
------
wsdbm__makesPurchase$$4$$	1	SO	wsdbm__makesPurchase/sorg__author
	VP	<wsdbm__makesPurchase>	1499988
	SO	<wsdbm__makesPurchase><sorg__author>	32726	0.02
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	1499988	1.0
------
wsdbm__purchaseFor$$5$$	2	OS	wsdbm__purchaseFor/sorg__contentRating
	VP	<wsdbm__purchaseFor>	1500000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	1499988	1.0
	OS	<wsdbm__purchaseFor><sorg__contentRating>	423972	0.28
------
