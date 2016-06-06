SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v1 
	 FROM gr__offers$$1$$ 
	 WHERE sub = 'wsdbm:Retailer6023'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM gr__includes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__director$$3$$
	
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
	 FROM wsdbm__likes$$6$$
	) tab5
 ON(tab4.v5=tab5.v5)
 JOIN    (SELECT obj AS v7 , sub AS v6 
	 FROM sorg__editor$$7$$
	) tab6
 ON(tab5.v6=tab6.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM wsdbm__makesPurchase$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM wsdbm__purchaseFor$$9$$
	
	) tab8
 ON(tab7.v8=tab8.v8)
 JOIN    (SELECT sub AS v9 , obj AS v10 
	 FROM sorg__caption$$10$$
	
	) tab9
 ON(tab8.v9=tab9.v9)


++++++Tables Statistic
wsdbm__likes$$6$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	1124672
------
sorg__caption$$10$$	0	VP	sorg__caption/
	VP	<sorg__caption>	24836
------
wsdbm__purchaseFor$$9$$	0	VP	wsdbm__purchaseFor/
	VP	<wsdbm__purchaseFor>	1500000
------
sorg__director$$3$$	0	VP	sorg__director/
	VP	<sorg__director>	13212
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	1420053
------
sorg__editor$$7$$	0	VP	sorg__editor/
	VP	<sorg__editor>	16107
------
wsdbm__friendOf$$4$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	45092208
------
gr__includes$$2$$	0	VP	gr__includes/
	VP	<gr__includes>	900000
------
wsdbm__makesPurchase$$8$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	1499988
------
wsdbm__friendOf$$5$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	45092208
------
