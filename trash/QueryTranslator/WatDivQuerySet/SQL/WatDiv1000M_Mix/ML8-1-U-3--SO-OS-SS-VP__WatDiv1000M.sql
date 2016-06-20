SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__likes$$1$$ 
	 WHERE sub = 'wsdbm:User1539505'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM rev__hasReview$$2$$
	
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM rev__reviewer$$3$$
	
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
	 FROM mo__artist$$6$$
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM wsdbm__friendOf$$7$$
	
	) tab6
 ON(tab5.v6=tab6.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM foaf__homepage$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)


++++++Tables Statistic
rev__hasReview$$2$$	1	SO	rev__hasReview/wsdbm__likes
	VP	<rev__hasReview>	14789439
	SO	<rev__hasReview><wsdbm__likes>	13979801	0.95
	OS	<rev__hasReview><rev__reviewer>	14789439	1.0
------
wsdbm__friendOf$$7$$	1	SO	wsdbm__friendOf/mo__artist
	VP	<wsdbm__friendOf>	449969341
	SO	<wsdbm__friendOf><mo__artist>	2316303	0.01
	OS	<wsdbm__friendOf><foaf__homepage>	22151163	0.05
------
wsdbm__likes$$1$$	1	OS	wsdbm__likes/rev__hasReview
	VP	<wsdbm__likes>	11246476
	OS	<wsdbm__likes><rev__hasReview>	2148995	0.19
------
rev__reviewer$$3$$	2	OS	rev__reviewer/wsdbm__makesPurchase
	VP	<rev__reviewer>	15000000
	SO	<rev__reviewer><rev__hasReview>	14789439	0.99
	OS	<rev__reviewer><wsdbm__makesPurchase>	769924	0.05
------
wsdbm__makesPurchase$$4$$	1	SO	wsdbm__makesPurchase/rev__reviewer
	VP	<wsdbm__makesPurchase>	14999930
	SO	<wsdbm__makesPurchase><rev__reviewer>	1504098	0.1
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	14999930	1.0
------
mo__artist$$6$$	2	OS	mo__artist/wsdbm__friendOf
	VP	<mo__artist>	132709
	SO	<mo__artist><wsdbm__purchaseFor>	80139	0.6
	OS	<mo__artist><wsdbm__friendOf>	50410	0.38
------
foaf__homepage$$8$$	1	SO	foaf__homepage/wsdbm__friendOf
	VP	<foaf__homepage>	1118496
	SO	<foaf__homepage><wsdbm__friendOf>	492338	0.44
------
wsdbm__purchaseFor$$5$$	2	OS	wsdbm__purchaseFor/mo__artist
	VP	<wsdbm__purchaseFor>	15000000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	14999930	1.0
	OS	<wsdbm__purchaseFor><mo__artist>	1184150	0.08
------
