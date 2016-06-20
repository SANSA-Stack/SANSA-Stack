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
wsdbm__likes$$6$$	2	OS	wsdbm__likes/sorg__author
	VP	<wsdbm__likes>	112401
	SO	<wsdbm__likes><wsdbm__friendOf>	112401	1.0
	OS	<wsdbm__likes><sorg__author>	11978	0.11
------
sorg__language$$10$$	1	SO	sorg__language/foaf__homepage
	VP	<sorg__language>	6251
	SO	<sorg__language><foaf__homepage>	4983	0.8
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	119316
	OS	<gr__offers><gr__includes>	119316	1.0
------
sorg__author$$7$$	2	OS	sorg__author/wsdbm__follows
	VP	<sorg__author>	3975
	SO	<sorg__author><wsdbm__likes>	3786	0.95
	OS	<sorg__author><wsdbm__follows>	3081	0.78
------
foaf__homepage$$9$$	1	SO	foaf__homepage/wsdbm__follows
	VP	<foaf__homepage>	11204
	SO	<foaf__homepage><wsdbm__follows>	4468	0.4
	OS	<foaf__homepage><sorg__language>	11204	1.0
------
rev__reviewer$$4$$	2	OS	rev__reviewer/wsdbm__friendOf
	VP	<rev__reviewer>	150000
	SO	<rev__reviewer><rev__hasReview>	149634	1.0
	OS	<rev__reviewer><wsdbm__friendOf>	59279	0.4
------
gr__includes$$2$$	2	OS	gr__includes/rev__hasReview
	VP	<gr__includes>	90000
	SO	<gr__includes><gr__offers>	44841	0.5
	OS	<gr__includes><rev__hasReview>	18580	0.21
------
rev__hasReview$$3$$	1	SO	rev__hasReview/gr__includes
	VP	<rev__hasReview>	149634
	SO	<rev__hasReview><gr__includes>	145435	0.97
	OS	<rev__hasReview><rev__reviewer>	149634	1.0
------
wsdbm__follows$$8$$	1	SO	wsdbm__follows/sorg__author
	VP	<wsdbm__follows>	3289307
	SO	<wsdbm__follows><sorg__author>	120783	0.04
	OS	<wsdbm__follows><foaf__homepage>	158692	0.05
------
wsdbm__friendOf$$5$$	2	OS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	4491142
	SO	<wsdbm__friendOf><rev__reviewer>	1381238	0.31
	OS	<wsdbm__friendOf><wsdbm__likes>	1074144	0.24
------
