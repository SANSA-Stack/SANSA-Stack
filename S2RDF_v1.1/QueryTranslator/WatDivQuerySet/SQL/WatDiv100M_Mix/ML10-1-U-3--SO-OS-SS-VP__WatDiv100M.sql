SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User595828'
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
	VP	<wsdbm__likes>	1124672
	SO	<wsdbm__likes><wsdbm__follows>	1020726	0.91
	OS	<wsdbm__likes><mo__artist>	58407	0.05
------
wsdbm__likes$$9$$	2	OS	wsdbm__likes/foaf__homepage
	VP	<wsdbm__likes>	1124672
	SO	<wsdbm__likes><wsdbm__friendOf>	1124672	1.0
	OS	<wsdbm__likes><foaf__homepage>	249032	0.22
------
wsdbm__follows$$5$$	1	SO	wsdbm__follows/sorg__author
	VP	<wsdbm__follows>	32736135
	SO	<wsdbm__follows><sorg__author>	1243389	0.04
	OS	<wsdbm__follows><wsdbm__likes>	7794356	0.24
------
wsdbm__friendOf$$8$$	1	SO	wsdbm__friendOf/mo__artist
	VP	<wsdbm__friendOf>	45092208
	SO	<wsdbm__friendOf><mo__artist>	266771	0.01
	OS	<wsdbm__friendOf><wsdbm__likes>	10736619	0.24
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__makesPurchase
	VP	<wsdbm__friendOf>	45092208
	OS	<wsdbm__friendOf><wsdbm__makesPurchase>	7128374	0.16
------
foaf__homepage$$10$$	1	SO	foaf__homepage/wsdbm__likes
	VP	<foaf__homepage>	111711
	SO	<foaf__homepage><wsdbm__likes>	59124	0.53
------
sorg__author$$4$$	1	SO	sorg__author/wsdbm__purchaseFor
	VP	<sorg__author>	40060
	SO	<sorg__author><wsdbm__purchaseFor>	26093	0.65
	OS	<sorg__author><wsdbm__follows>	31146	0.78
------
mo__artist$$7$$	2	OS	mo__artist/wsdbm__friendOf
	VP	<mo__artist>	13306
	SO	<mo__artist><wsdbm__likes>	12601	0.95
	OS	<mo__artist><wsdbm__friendOf>	5227	0.39
------
wsdbm__purchaseFor$$3$$	2	OS	wsdbm__purchaseFor/sorg__author
	VP	<wsdbm__purchaseFor>	1500000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	1499988	1.0
	OS	<wsdbm__purchaseFor><sorg__author>	119127	0.08
------
wsdbm__makesPurchase$$2$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	1499988
	SO	<wsdbm__makesPurchase><wsdbm__friendOf>	1499988	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	1499988	1.0
------
