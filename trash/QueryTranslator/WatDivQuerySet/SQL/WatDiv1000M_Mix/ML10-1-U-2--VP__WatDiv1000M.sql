SELECT tab0.v1 AS v1 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab8.v9 AS v9 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__friendOf$$1$$
	 
	 WHERE sub = 'wsdbm:User7771442'
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
wsdbm__likes$$6$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	11246476
------
wsdbm__likes$$9$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	11246476
------
wsdbm__follows$$5$$	0	VP	wsdbm__follows/
	VP	<wsdbm__follows>	327487530
------
wsdbm__friendOf$$8$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	449969341
------
wsdbm__friendOf$$1$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	449969341
------
foaf__homepage$$10$$	0	VP	foaf__homepage/
	VP	<foaf__homepage>	1118496
------
sorg__author$$4$$	0	VP	sorg__author/
	VP	<sorg__author>	399974
------
mo__artist$$7$$	0	VP	mo__artist/
	VP	<mo__artist>	132709
------
wsdbm__purchaseFor$$3$$	0	VP	wsdbm__purchaseFor/
	VP	<wsdbm__purchaseFor>	15000000
------
wsdbm__makesPurchase$$2$$	0	VP	wsdbm__makesPurchase/
	VP	<wsdbm__makesPurchase>	14999930
------
