SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__follows$$1$$
	 
	 WHERE sub = 'wsdbm:User479483'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM rev__hasReview$$3$$
	
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM rev__reviewer$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__friendOf$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__makesPurchase$$6$$
	
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM wsdbm__purchaseFor$$7$$
	
	) tab6
 ON(tab5.v6=tab6.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM sorg__author$$8$$
	) tab7
 ON(tab6.v7=tab7.v7)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM dc__Location$$9$$
	) tab8
 ON(tab7.v8=tab8.v8)


++++++Tables Statistic
sorg__author$$8$$	2	OS	sorg__author/dc__Location
	VP	<sorg__author>	40060
	SO	<sorg__author><wsdbm__purchaseFor>	26093	0.65
	OS	<sorg__author><dc__Location>	16096	0.4
------
wsdbm__makesPurchase$$6$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	1499988
	SO	<wsdbm__makesPurchase><wsdbm__friendOf>	1499988	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	1499988	1.0
------
dc__Location$$9$$	1	SO	dc__Location/sorg__author
	VP	<dc__Location>	400344
	SO	<dc__Location><sorg__author>	15273	0.04
------
wsdbm__likes$$2$$	2	OS	wsdbm__likes/rev__hasReview
	VP	<wsdbm__likes>	1124672
	SO	<wsdbm__likes><wsdbm__follows>	1020726	0.91
	OS	<wsdbm__likes><rev__hasReview>	219867	0.2
------
wsdbm__purchaseFor$$7$$	2	OS	wsdbm__purchaseFor/sorg__author
	VP	<wsdbm__purchaseFor>	1500000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	1499988	1.0
	OS	<wsdbm__purchaseFor><sorg__author>	119127	0.08
------
rev__reviewer$$4$$	2	OS	rev__reviewer/wsdbm__friendOf
	VP	<rev__reviewer>	1500000
	SO	<rev__reviewer><rev__hasReview>	1476843	0.98
	OS	<rev__reviewer><wsdbm__friendOf>	601949	0.4
------
rev__hasReview$$3$$	1	SO	rev__hasReview/wsdbm__likes
	VP	<rev__hasReview>	1476843
	SO	<rev__hasReview><wsdbm__likes>	1402411	0.95
	OS	<rev__hasReview><rev__reviewer>	1476843	1.0
------
wsdbm__follows$$1$$	1	OS	wsdbm__follows/wsdbm__likes
	VP	<wsdbm__follows>	32736135
	OS	<wsdbm__follows><wsdbm__likes>	7794356	0.24
------
wsdbm__friendOf$$5$$	2	OS	wsdbm__friendOf/wsdbm__makesPurchase
	VP	<wsdbm__friendOf>	45092208
	SO	<wsdbm__friendOf><rev__reviewer>	13968837	0.31
	OS	<wsdbm__friendOf><wsdbm__makesPurchase>	7128374	0.16
------
