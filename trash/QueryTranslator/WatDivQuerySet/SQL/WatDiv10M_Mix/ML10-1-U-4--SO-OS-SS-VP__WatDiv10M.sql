SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User14017'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__makesPurchase$$2$$
	
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM wsdbm__purchaseFor$$3$$
	
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM sorg__author$$4$$
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__follows$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__likes$$6$$
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM mo__artist$$7$$
	) tab6
 ON(tab5.v6=tab6.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM wsdbm__friendOf$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM wsdbm__likes$$9$$
	) tab8
 ON(tab7.v8=tab8.v8)
 JOIN    (SELECT sub AS v9 , obj AS v10 
	 FROM foaf__homepage$$10$$
	
	) tab9
 ON(tab8.v9=tab9.v9)


++++++Tables Statistic
wsdbm__likes$$6$$	2	OS	wsdbm__likes/mo__artist
	VP	<wsdbm__likes>	112401
	SO	<wsdbm__likes><wsdbm__follows>	102458	0.91
	OS	<wsdbm__likes><mo__artist>	5906	0.05
------
wsdbm__likes$$9$$	2	OS	wsdbm__likes/foaf__homepage
	VP	<wsdbm__likes>	112401
	SO	<wsdbm__likes><wsdbm__friendOf>	112401	1.0
	OS	<wsdbm__likes><foaf__homepage>	26377	0.23
------
wsdbm__follows$$5$$	1	SO	wsdbm__follows/sorg__author
	VP	<wsdbm__follows>	3289307
	SO	<wsdbm__follows><sorg__author>	120783	0.04
	OS	<wsdbm__follows><wsdbm__likes>	787951	0.24
------
wsdbm__friendOf$$8$$	1	SO	wsdbm__friendOf/mo__artist
	VP	<wsdbm__friendOf>	4491142
	SO	<wsdbm__friendOf><mo__artist>	29838	0.01
	OS	<wsdbm__friendOf><wsdbm__likes>	1074144	0.24
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__makesPurchase
	VP	<wsdbm__friendOf>	4491142
	OS	<wsdbm__friendOf><wsdbm__makesPurchase>	710206	0.16
------
foaf__homepage$$10$$	1	SO	foaf__homepage/wsdbm__likes
	VP	<foaf__homepage>	11204
	SO	<foaf__homepage><wsdbm__likes>	5961	0.53
------
sorg__author$$4$$	1	SO	sorg__author/wsdbm__purchaseFor
	VP	<sorg__author>	3975
	SO	<sorg__author><wsdbm__purchaseFor>	2833	0.71
	OS	<sorg__author><wsdbm__follows>	3081	0.78
------
mo__artist$$7$$	2	OS	mo__artist/wsdbm__friendOf
	VP	<mo__artist>	1335
	SO	<mo__artist><wsdbm__likes>	1267	0.95
	OS	<mo__artist><wsdbm__friendOf>	577	0.43
------
wsdbm__purchaseFor$$3$$	2	OS	wsdbm__purchaseFor/sorg__author
	VP	<wsdbm__purchaseFor>	150000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	149998	1.0
	OS	<wsdbm__purchaseFor><sorg__author>	15762	0.11
------
wsdbm__makesPurchase$$2$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	149998
	SO	<wsdbm__makesPurchase><wsdbm__friendOf>	149998	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	149998	1.0
------
