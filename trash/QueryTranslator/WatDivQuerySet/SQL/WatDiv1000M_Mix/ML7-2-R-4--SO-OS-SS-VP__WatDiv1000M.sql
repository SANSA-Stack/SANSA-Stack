SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM sorg__contactPoint$$1$$
	 
	 WHERE sub = 'wsdbm:Retailer111171'
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
	 FROM wsdbm__follows$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__makesPurchase$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__purchaseFor$$6$$
	
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM og__tag$$7$$
	) tab6
 ON(tab5.v6=tab6.v6)


++++++Tables Statistic
og__tag$$7$$	1	SO	og__tag/wsdbm__purchaseFor
	VP	<og__tag>	14987949
	SO	<og__tag><wsdbm__purchaseFor>	9063453	0.6
------
wsdbm__makesPurchase$$5$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	14999930
	SO	<wsdbm__makesPurchase><wsdbm__follows>	14999930	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	14999930	1.0
------
wsdbm__follows$$4$$	1	SO	wsdbm__follows/sorg__author
	VP	<wsdbm__follows>	327487530
	SO	<wsdbm__follows><sorg__author>	12444466	0.04
	OS	<wsdbm__follows><wsdbm__makesPurchase>	82213305	0.25
------
wsdbm__purchaseFor$$6$$	2	OS	wsdbm__purchaseFor/og__tag
	VP	<wsdbm__purchaseFor>	15000000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	14999930	1.0
	OS	<wsdbm__purchaseFor><og__tag>	9929177	0.66
------
sorg__contactPoint$$1$$	1	OS	sorg__contactPoint/wsdbm__likes
	VP	<sorg__contactPoint>	95822
	OS	<sorg__contactPoint><wsdbm__likes>	22864	0.24
------
sorg__author$$3$$	2	OS	sorg__author/wsdbm__follows
	VP	<sorg__author>	399974
	SO	<sorg__author><wsdbm__likes>	377768	0.94
	OS	<sorg__author><wsdbm__follows>	310200	0.78
------
wsdbm__likes$$2$$	1	SO	wsdbm__likes/sorg__contactPoint
	VP	<wsdbm__likes>	11246476
	SO	<wsdbm__likes><sorg__contactPoint>	108023	0.01
	OS	<wsdbm__likes><sorg__author>	1428651	0.13
------
