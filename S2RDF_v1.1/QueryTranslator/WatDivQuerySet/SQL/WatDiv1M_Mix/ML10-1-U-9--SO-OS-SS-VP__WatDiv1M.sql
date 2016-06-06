SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User5975'
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
	VP	<wsdbm__likes>	11256
	SO	<wsdbm__likes><wsdbm__follows>	10495	0.93
	OS	<wsdbm__likes><mo__artist>	478	0.04
------
wsdbm__likes$$9$$	2	OS	wsdbm__likes/foaf__homepage
	VP	<wsdbm__likes>	11256
	SO	<wsdbm__likes><wsdbm__friendOf>	11256	1.0
	OS	<wsdbm__likes><foaf__homepage>	2677	0.24
------
wsdbm__follows$$5$$	1	SO	wsdbm__follows/sorg__author
	VP	<wsdbm__follows>	330403
	SO	<wsdbm__follows><sorg__author>	11555	0.03
	OS	<wsdbm__follows><wsdbm__likes>	78618	0.24
------
wsdbm__friendOf$$8$$	1	SO	wsdbm__friendOf/mo__artist
	VP	<wsdbm__friendOf>	448135
	SO	<wsdbm__friendOf><mo__artist>	3329	0.01
	OS	<wsdbm__friendOf><wsdbm__likes>	106881	0.24
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__makesPurchase
	VP	<wsdbm__friendOf>	448135
	OS	<wsdbm__friendOf><wsdbm__makesPurchase>	71148	0.16
------
foaf__homepage$$10$$	1	SO	foaf__homepage/wsdbm__likes
	VP	<foaf__homepage>	1068
	SO	<foaf__homepage><wsdbm__likes>	553	0.52
------
sorg__author$$4$$	2	OS	sorg__author/wsdbm__follows
	VP	<sorg__author>	367
	SO	<sorg__author><wsdbm__purchaseFor>	285	0.78
	OS	<sorg__author><wsdbm__follows>	279	0.76
------
mo__artist$$7$$	2	OS	mo__artist/wsdbm__friendOf
	VP	<mo__artist>	115
	SO	<mo__artist><wsdbm__likes>	113	0.98
	OS	<mo__artist><wsdbm__friendOf>	35	0.3
------
wsdbm__purchaseFor$$3$$	2	OS	wsdbm__purchaseFor/sorg__author
	VP	<wsdbm__purchaseFor>	15000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	15000	1.0
	OS	<wsdbm__purchaseFor><sorg__author>	1313	0.09
------
wsdbm__makesPurchase$$2$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	15000
	SO	<wsdbm__makesPurchase><wsdbm__friendOf>	15000	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	15000	1.0
------
