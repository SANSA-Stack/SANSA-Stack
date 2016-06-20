SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__makesPurchase$$1$$
	 
	 WHERE sub = 'wsdbm:User36840'
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
	VP	<wsdbm__makesPurchase>	149998
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	149998	1.0
------
wsdbm__follows$$5$$	2	OS	wsdbm__follows/wsdbm__subscribes
	VP	<wsdbm__follows>	3289307
	SO	<wsdbm__follows><rev__reviewer>	1015259	0.31
	OS	<wsdbm__follows><wsdbm__subscribes>	663249	0.2
------
wsdbm__subscribes$$6$$	1	SO	wsdbm__subscribes/wsdbm__follows
	VP	<wsdbm__subscribes>	152275
	SO	<wsdbm__subscribes><wsdbm__follows>	139314	0.91
	OS	<wsdbm__subscribes><sorg__language>	152275	1.0
------
rev__reviewer$$4$$	2	OS	rev__reviewer/wsdbm__follows
	VP	<rev__reviewer>	150000
	SO	<rev__reviewer><rev__hasReview>	149634	1.0
	OS	<rev__reviewer><wsdbm__follows>	116416	0.78
------
sorg__language$$7$$	1	SO	sorg__language/wsdbm__subscribes
	VP	<sorg__language>	6251
	SO	<sorg__language><wsdbm__subscribes>	5000	0.8
------
rev__hasReview$$3$$	1	SO	rev__hasReview/wsdbm__purchaseFor
	VP	<rev__hasReview>	149634
	SO	<rev__hasReview><wsdbm__purchaseFor>	107006	0.72
	OS	<rev__hasReview><rev__reviewer>	149634	1.0
------
wsdbm__purchaseFor$$2$$	2	OS	wsdbm__purchaseFor/rev__hasReview
	VP	<wsdbm__purchaseFor>	150000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	149998	1.0
	OS	<wsdbm__purchaseFor><rev__hasReview>	33884	0.23
------
