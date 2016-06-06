SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab5.v5 AS v5 , tab6.v7 AS v7 , tab4.v4 AS v4 , tab6.v6 AS v6 , tab3.v3 AS v3 , tab8.v9 AS v9 , tab2.v2 AS v2 , tab7.v8 AS v8 , tab9.v10 AS v10 
 FROM    (SELECT obj AS v7 , sub AS v6 
	 FROM sorg__author$$7$$
	) tab6
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__likes$$6$$
	) tab5
 ON(tab6.v6=tab5.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM wsdbm__follows$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)
 JOIN    (SELECT obj AS v9 , sub AS v8 
	 FROM foaf__homepage$$9$$
	
	) tab8
 ON(tab7.v8=tab8.v8)
 JOIN    (SELECT sub AS v9 , obj AS v10 
	 FROM sorg__language$$10$$
	
	) tab9
 ON(tab8.v9=tab9.v9)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__friendOf$$5$$
	
	) tab4
 ON(tab5.v5=tab4.v5)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM rev__reviewer$$4$$
	
	) tab3
 ON(tab4.v4=tab3.v4)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM rev__hasReview$$3$$
	
	) tab2
 ON(tab3.v3=tab2.v3)
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM gr__includes$$2$$
	) tab1
 ON(tab2.v2=tab1.v2)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM gr__offers$$1$$
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
wsdbm__likes$$6$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	11246476
------
sorg__language$$10$$	0	VP	sorg__language/
	VP	<sorg__language>	641195
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	14156906
------
sorg__author$$7$$	0	VP	sorg__author/
	VP	<sorg__author>	399974
------
foaf__homepage$$9$$	0	VP	foaf__homepage/
	VP	<foaf__homepage>	1118496
------
rev__reviewer$$4$$	0	VP	rev__reviewer/
	VP	<rev__reviewer>	15000000
------
gr__includes$$2$$	0	VP	gr__includes/
	VP	<gr__includes>	9000000
------
rev__hasReview$$3$$	0	VP	rev__hasReview/
	VP	<rev__hasReview>	14789439
------
wsdbm__follows$$8$$	0	VP	wsdbm__follows/
	VP	<wsdbm__follows>	327487530
------
wsdbm__friendOf$$5$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	449969341
------
