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
sorg__actor$$8$$	0	VP	sorg__actor/
	VP	<sorg__actor>	1668
------
wsdbm__makesPurchase$$6$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	15000
------
sorg__language$$10$$	0	VP	sorg__language/
	VP	<sorg__language>	655
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	14179
------
wsdbm__subscribes$$9$$	0	VP	wsdbm__subscribes/
	VP	<wsdbm__subscribes>	14908
------
wsdbm__purchaseFor$$7$$	0	VP	wsdbm__purchaseFor/
	VP	<wsdbm__purchaseFor>	15000
------
rev__reviewer$$4$$	0	VP	rev__reviewer/
	VP	<rev__reviewer>	15000
------
gr__includes$$2$$	0	VP	gr__includes/
	VP	<gr__includes>	9000
------
rev__hasReview$$3$$	0	VP	rev__hasReview/
	VP	<rev__hasReview>	14757
------
wsdbm__friendOf$$5$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	448135
------
