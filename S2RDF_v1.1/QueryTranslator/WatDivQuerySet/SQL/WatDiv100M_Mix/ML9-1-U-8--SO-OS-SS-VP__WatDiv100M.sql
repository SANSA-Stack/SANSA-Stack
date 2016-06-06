SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__follows$$1$$
	 
	 WHERE sub = 'wsdbm:User345724'
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
	 FROM rev__hasReview$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM rev__reviewer$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__likes$$6$$
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM sorg__actor$$7$$
	) tab6
 ON(tab5.v6=tab6.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM wsdbm__friendOf$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM sorg__telephone$$9$$
	
	) tab8
 ON(tab7.v8=tab8.v8)


++++++Tables Statistic
sorg__actor$$7$$	2	OS	sorg__actor/wsdbm__friendOf
	VP	<sorg__actor>	164597
	SO	<sorg__actor><wsdbm__likes>	156153	0.95
	OS	<sorg__actor><wsdbm__friendOf>	65870	0.4
------
wsdbm__likes$$6$$	2	OS	wsdbm__likes/sorg__actor
	VP	<wsdbm__likes>	1124672
	SO	<wsdbm__likes><rev__reviewer>	345355	0.31
	OS	<wsdbm__likes><sorg__actor>	56797	0.05
------
wsdbm__friendOf$$8$$	2	OS	wsdbm__friendOf/sorg__telephone
	VP	<wsdbm__friendOf>	45092208
	SO	<wsdbm__friendOf><sorg__actor>	5160102	0.11
	OS	<wsdbm__friendOf><sorg__telephone>	2252552	0.05
------
rev__reviewer$$5$$	2	OS	rev__reviewer/wsdbm__likes
	VP	<rev__reviewer>	1500000
	SO	<rev__reviewer><rev__hasReview>	1476843	0.98
	OS	<rev__reviewer><wsdbm__likes>	356907	0.24
------
rev__hasReview$$4$$	1	SO	rev__hasReview/wsdbm__purchaseFor
	VP	<rev__hasReview>	1476843
	SO	<rev__hasReview><wsdbm__purchaseFor>	961311	0.65
	OS	<rev__hasReview><rev__reviewer>	1476843	1.0
------
wsdbm__purchaseFor$$3$$	2	OS	wsdbm__purchaseFor/rev__hasReview
	VP	<wsdbm__purchaseFor>	1500000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	1499988	1.0
	OS	<wsdbm__purchaseFor><rev__hasReview>	284620	0.19
------
wsdbm__makesPurchase$$2$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	1499988
	SO	<wsdbm__makesPurchase><wsdbm__follows>	1499988	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	1499988	1.0
------
sorg__telephone$$9$$	1	SO	sorg__telephone/wsdbm__friendOf
	VP	<sorg__telephone>	58343
	SO	<sorg__telephone><wsdbm__friendOf>	49943	0.86
------
wsdbm__follows$$1$$	1	OS	wsdbm__follows/wsdbm__makesPurchase
	VP	<wsdbm__follows>	32736135
	OS	<wsdbm__follows><wsdbm__makesPurchase>	8208874	0.25
------
