SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM gr__offers$$1$$ 
	 WHERE sub = 'wsdbm:Retailer250'
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
	 FROM wsdbm__likes$$5$$
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM sorg__description$$6$$
	
	) tab5
 ON(tab4.v5=tab5.v5)


++++++Tables Statistic
sorg__description$$6$$	1	SO	sorg__description/wsdbm__likes
	VP	<sorg__description>	14960
	SO	<sorg__description><wsdbm__likes>	14248	0.95
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	119316
	OS	<gr__offers><gr__includes>	119316	1.0
------
rev__reviewer$$4$$	2	OS	rev__reviewer/wsdbm__likes
	VP	<rev__reviewer>	150000
	SO	<rev__reviewer><rev__hasReview>	149634	1.0
	OS	<rev__reviewer><wsdbm__likes>	35289	0.24
------
gr__includes$$2$$	2	OS	gr__includes/rev__hasReview
	VP	<gr__includes>	90000
	SO	<gr__includes><gr__offers>	44841	0.5
	OS	<gr__includes><rev__hasReview>	18580	0.21
------
wsdbm__likes$$5$$	1	SO	wsdbm__likes/rev__reviewer
	VP	<wsdbm__likes>	112401
	SO	<wsdbm__likes><rev__reviewer>	34354	0.31
	OS	<wsdbm__likes><sorg__description>	69713	0.62
------
rev__hasReview$$3$$	1	SO	rev__hasReview/gr__includes
	VP	<rev__hasReview>	149634
	SO	<rev__hasReview><gr__includes>	145435	0.97
	OS	<rev__hasReview><rev__reviewer>	149634	1.0
------
