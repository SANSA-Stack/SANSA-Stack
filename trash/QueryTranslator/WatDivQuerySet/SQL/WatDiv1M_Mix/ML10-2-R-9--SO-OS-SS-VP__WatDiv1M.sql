SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v1 
	 FROM gr__offers$$1$$ 
	 WHERE sub = 'wsdbm:Retailer44'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM gr__includes$$2$$
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
	 FROM sorg__actor$$8$$
	) tab7
 ON(tab6.v7=tab7.v7)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM wsdbm__subscribes$$9$$
	
	) tab8
 ON(tab7.v8=tab8.v8)
 JOIN    (SELECT sub AS v9 , obj AS v10 
	 FROM sorg__language$$10$$
	
	) tab9
 ON(tab8.v9=tab9.v9)


++++++Tables Statistic
sorg__actor$$8$$	2	OS	sorg__actor/wsdbm__subscribes
	VP	<sorg__actor>	1668
	SO	<sorg__actor><wsdbm__purchaseFor>	1339	0.8
	OS	<sorg__actor><wsdbm__subscribes>	335	0.2
------
wsdbm__makesPurchase$$6$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	15000
	SO	<wsdbm__makesPurchase><wsdbm__friendOf>	15000	1.0
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	15000	1.0
------
sorg__language$$10$$	1	SO	sorg__language/wsdbm__subscribes
	VP	<sorg__language>	655
	SO	<sorg__language><wsdbm__subscribes>	500	0.76
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	14179
	OS	<gr__offers><gr__includes>	14179	1.0
------
wsdbm__subscribes$$9$$	1	SO	wsdbm__subscribes/sorg__actor
	VP	<wsdbm__subscribes>	14908
	SO	<wsdbm__subscribes><sorg__actor>	1719	0.12
	OS	<wsdbm__subscribes><sorg__language>	14908	1.0
------
wsdbm__purchaseFor$$7$$	2	OS	wsdbm__purchaseFor/sorg__actor
	VP	<wsdbm__purchaseFor>	15000
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	15000	1.0
	OS	<wsdbm__purchaseFor><sorg__actor>	1231	0.08
------
rev__reviewer$$4$$	2	OS	rev__reviewer/wsdbm__friendOf
	VP	<rev__reviewer>	15000
	SO	<rev__reviewer><rev__hasReview>	14757	0.98
	OS	<rev__reviewer><wsdbm__friendOf>	5835	0.39
------
gr__includes$$2$$	2	OS	gr__includes/rev__hasReview
	VP	<gr__includes>	9000
	SO	<gr__includes><gr__offers>	4722	0.52
	OS	<gr__includes><rev__hasReview>	1837	0.2
------
rev__hasReview$$3$$	1	SO	rev__hasReview/gr__includes
	VP	<rev__hasReview>	14757
	SO	<rev__hasReview><gr__includes>	14349	0.97
	OS	<rev__hasReview><rev__reviewer>	14757	1.0
------
wsdbm__friendOf$$5$$	2	OS	wsdbm__friendOf/wsdbm__makesPurchase
	VP	<wsdbm__friendOf>	448135
	SO	<wsdbm__friendOf><rev__reviewer>	133496	0.3
	OS	<wsdbm__friendOf><wsdbm__makesPurchase>	71148	0.16
------
