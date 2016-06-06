SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM wsdbm__follows$$1$$
	 
	 WHERE sub = 'wsdbm:User6707'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__likes$$2$$
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
	 FROM wsdbm__userId$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)


++++++Tables Statistic
wsdbm__likes$$2$$	2	OS	wsdbm__likes/rev__hasReview
	VP	<wsdbm__likes>	11256
	SO	<wsdbm__likes><wsdbm__follows>	10495	0.93
	OS	<wsdbm__likes><rev__hasReview>	2392	0.21
------
rev__reviewer$$4$$	1	SO	rev__reviewer/rev__hasReview
	VP	<rev__reviewer>	15000
	SO	<rev__reviewer><rev__hasReview>	14757	0.98
	OS	<rev__reviewer><wsdbm__userId>	15000	1.0
------
rev__hasReview$$3$$	1	SO	rev__hasReview/wsdbm__likes
	VP	<rev__hasReview>	14757
	SO	<rev__hasReview><wsdbm__likes>	14049	0.95
	OS	<rev__hasReview><rev__reviewer>	14757	1.0
------
wsdbm__userId$$5$$	1	SO	wsdbm__userId/rev__reviewer
	VP	<wsdbm__userId>	10000
	SO	<wsdbm__userId><rev__reviewer>	2996	0.3
------
wsdbm__follows$$1$$	1	OS	wsdbm__follows/wsdbm__likes
	VP	<wsdbm__follows>	330403
	OS	<wsdbm__follows><wsdbm__likes>	78618	0.24
------
