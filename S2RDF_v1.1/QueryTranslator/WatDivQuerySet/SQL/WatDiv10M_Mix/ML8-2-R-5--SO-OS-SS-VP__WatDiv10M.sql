SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM gr__offers$$1$$ 
	 WHERE sub = 'wsdbm:Retailer522'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM gr__includes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__author$$3$$
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM wsdbm__friendOf$$4$$
	
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
	 FROM foaf__homepage$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)


++++++Tables Statistic
wsdbm__makesPurchase$$6$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	149998
	SO	<wsdbm__makesPurchase><wsdbm__friendOf>	149998	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	149998	1.0
------
sorg__author$$3$$	2	OS	sorg__author/wsdbm__friendOf
	VP	<sorg__author>	3975
	SO	<sorg__author><gr__includes>	3854	0.97
	OS	<sorg__author><wsdbm__friendOf>	1588	0.4
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	119316
	OS	<gr__offers><gr__includes>	119316	1.0
------
wsdbm__purchaseFor$$7$$	2	OS	wsdbm__purchaseFor/foaf__homepage
	VP	<wsdbm__purchaseFor>	150000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	149998	1.0
	OS	<wsdbm__purchaseFor><foaf__homepage>	31129	0.21
------
wsdbm__friendOf$$4$$	1	SO	wsdbm__friendOf/sorg__author
	VP	<wsdbm__friendOf>	4491142
	SO	<wsdbm__friendOf><sorg__author>	170287	0.04
	OS	<wsdbm__friendOf><wsdbm__friendOf>	1787147	0.4
------
gr__includes$$2$$	2	OS	gr__includes/sorg__author
	VP	<gr__includes>	90000
	SO	<gr__includes><gr__offers>	44841	0.5
	OS	<gr__includes><sorg__author>	9380	0.1
------
foaf__homepage$$8$$	1	SO	foaf__homepage/wsdbm__purchaseFor
	VP	<foaf__homepage>	11204
	SO	<foaf__homepage><wsdbm__purchaseFor>	4415	0.39
------
wsdbm__friendOf$$5$$	2	OS	wsdbm__friendOf/wsdbm__makesPurchase
	VP	<wsdbm__friendOf>	4491142
	SO	<wsdbm__friendOf><wsdbm__friendOf>	4491142	1.0
	OS	<wsdbm__friendOf><wsdbm__makesPurchase>	710206	0.16
------
