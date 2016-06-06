SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__makesPurchase$$1$$
	 
	 WHERE sub = 'wsdbm:User9438279'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__purchaseFor$$2$$
	
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
	 FROM wsdbm__follows$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__subscribes$$6$$
	
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM sorg__language$$7$$
	
	) tab6
 ON(tab5.v6=tab6.v6)


++++++Tables Statistic
wsdbm__makesPurchase$$1$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	14999930
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	14999930	1.0
------
wsdbm__follows$$5$$	2	OS	wsdbm__follows/wsdbm__subscribes
	VP	<wsdbm__follows>	327487530
	SO	<wsdbm__follows><rev__reviewer>	101206556	0.31
	OS	<wsdbm__follows><wsdbm__subscribes>	65310881	0.2
------
wsdbm__subscribes$$6$$	1	SO	wsdbm__subscribes/wsdbm__follows
	VP	<wsdbm__subscribes>	14999080
	SO	<wsdbm__subscribes><wsdbm__follows>	13514809	0.9
	OS	<wsdbm__subscribes><sorg__language>	14999080	1.0
------
rev__reviewer$$4$$	2	OS	rev__reviewer/wsdbm__follows
	VP	<rev__reviewer>	15000000
	SO	<rev__reviewer><rev__hasReview>	14789439	0.99
	OS	<rev__reviewer><wsdbm__follows>	11621066	0.77
------
sorg__language$$7$$	1	SO	sorg__language/wsdbm__subscribes
	VP	<sorg__language>	641195
	SO	<sorg__language><wsdbm__subscribes>	500000	0.78
------
rev__hasReview$$3$$	1	SO	rev__hasReview/wsdbm__purchaseFor
	VP	<rev__hasReview>	14789439
	SO	<rev__hasReview><wsdbm__purchaseFor>	9000733	0.61
	OS	<rev__hasReview><rev__reviewer>	14789439	1.0
------
wsdbm__purchaseFor$$2$$	2	OS	wsdbm__purchaseFor/rev__hasReview
	VP	<wsdbm__purchaseFor>	15000000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	14999930	1.0
	OS	<wsdbm__purchaseFor><rev__hasReview>	2707055	0.18
------
