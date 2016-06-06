SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 , obj AS v3 
	 FROM dc__Location$$3$$
	) tab2
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM foaf__age$$4$$
	) tab3
 ON(tab2.v0=tab3.v0)
 JOIN    (SELECT sub AS v0 , obj AS v5 
	 FROM wsdbm__gender$$5$$
	
	) tab4
 ON(tab3.v0=tab4.v0)
 JOIN    (SELECT sub AS v0 , obj AS v6 
	 FROM foaf__givenName$$6$$
	
	) tab5
 ON(tab4.v0=tab5.v0)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__likes$$1$$
	) tab0
 ON(tab5.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM wsdbm__friendOf$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
dc__Location$$3$$	1	SS	dc__Location/wsdbm__likes
	VP	<dc__Location>	400344
	SS	<dc__Location><wsdbm__likes>	95020	0.24
	SS	<dc__Location><wsdbm__friendOf>	160182	0.4
	SS	<dc__Location><foaf__age>	200355	0.5
	SS	<dc__Location><wsdbm__gender>	240211	0.6
	SS	<dc__Location><foaf__givenName>	280431	0.7
------
foaf__givenName$$6$$	1	SS	foaf__givenName/wsdbm__likes
	VP	<foaf__givenName>	700284
	SS	<foaf__givenName><wsdbm__likes>	166663	0.24
	SS	<foaf__givenName><wsdbm__friendOf>	280446	0.4
	SS	<foaf__givenName><dc__Location>	280431	0.4
	SS	<foaf__givenName><foaf__age>	350487	0.5
	SS	<foaf__givenName><wsdbm__gender>	420404	0.6
------
wsdbm__likes$$1$$	2	SS	wsdbm__likes/dc__Location
	VP	<wsdbm__likes>	1124672
	SS	<wsdbm__likes><wsdbm__friendOf>	451799	0.4
	SS	<wsdbm__likes><dc__Location>	448797	0.4
	SS	<wsdbm__likes><foaf__age>	560100	0.5
	SS	<wsdbm__likes><wsdbm__gender>	675062	0.6
	SS	<wsdbm__likes><foaf__givenName>	788299	0.7
------
wsdbm__gender$$5$$	1	SS	wsdbm__gender/wsdbm__likes
	VP	<wsdbm__gender>	600332
	SS	<wsdbm__gender><wsdbm__likes>	143070	0.24
	SS	<wsdbm__gender><wsdbm__friendOf>	240544	0.4
	SS	<wsdbm__gender><dc__Location>	240211	0.4
	SS	<wsdbm__gender><foaf__age>	300061	0.5
	SS	<wsdbm__gender><foaf__givenName>	420404	0.7
------
wsdbm__friendOf$$2$$	1	SS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	45092208
	SS	<wsdbm__friendOf><wsdbm__likes>	10717258	0.24
	SS	<wsdbm__friendOf><dc__Location>	18024963	0.4
	SS	<wsdbm__friendOf><foaf__age>	22526553	0.5
	SS	<wsdbm__friendOf><wsdbm__gender>	27089950	0.6
	SS	<wsdbm__friendOf><foaf__givenName>	31570710	0.7
------
foaf__age$$4$$	1	SS	foaf__age/wsdbm__likes
	VP	<foaf__age>	500284
	SS	<foaf__age><wsdbm__likes>	119040	0.24
	SS	<foaf__age><wsdbm__friendOf>	200041	0.4
	SS	<foaf__age><dc__Location>	200355	0.4
	SS	<foaf__age><wsdbm__gender>	300061	0.6
	SS	<foaf__age><foaf__givenName>	350487	0.7
------
