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
	VP	<dc__Location>	3925
	SS	<dc__Location><wsdbm__likes>	936	0.24
	SS	<dc__Location><wsdbm__friendOf>	1543	0.39
	SS	<dc__Location><foaf__age>	1948	0.5
	SS	<dc__Location><wsdbm__gender>	2384	0.61
	SS	<dc__Location><foaf__givenName>	2736	0.7
------
foaf__givenName$$6$$	1	SS	foaf__givenName/wsdbm__likes
	VP	<foaf__givenName>	6968
	SS	<foaf__givenName><wsdbm__likes>	1690	0.24
	SS	<foaf__givenName><wsdbm__friendOf>	2762	0.4
	SS	<foaf__givenName><dc__Location>	2736	0.39
	SS	<foaf__givenName><foaf__age>	3524	0.51
	SS	<foaf__givenName><wsdbm__gender>	4222	0.61
------
wsdbm__likes$$1$$	1	SS	wsdbm__likes/wsdbm__friendOf
	VP	<wsdbm__likes>	11256
	SS	<wsdbm__likes><wsdbm__friendOf>	4481	0.4
	SS	<wsdbm__likes><dc__Location>	4618	0.41
	SS	<wsdbm__likes><foaf__age>	5780	0.51
	SS	<wsdbm__likes><wsdbm__gender>	6564	0.58
	SS	<wsdbm__likes><foaf__givenName>	7696	0.68
------
wsdbm__gender$$5$$	1	SS	wsdbm__gender/wsdbm__likes
	VP	<wsdbm__gender>	6053
	SS	<wsdbm__gender><wsdbm__likes>	1411	0.23
	SS	<wsdbm__gender><wsdbm__friendOf>	2387	0.39
	SS	<wsdbm__gender><dc__Location>	2384	0.39
	SS	<wsdbm__gender><foaf__age>	3053	0.5
	SS	<wsdbm__gender><foaf__givenName>	4222	0.7
------
wsdbm__friendOf$$2$$	1	SS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	448135
	SS	<wsdbm__friendOf><wsdbm__likes>	106305	0.24
	SS	<wsdbm__friendOf><dc__Location>	174514	0.39
	SS	<wsdbm__friendOf><foaf__age>	223846	0.5
	SS	<wsdbm__friendOf><wsdbm__gender>	269568	0.6
	SS	<wsdbm__friendOf><foaf__givenName>	314144	0.7
------
foaf__age$$4$$	1	SS	foaf__age/wsdbm__likes
	VP	<foaf__age>	5054
	SS	<foaf__age><wsdbm__likes>	1231	0.24
	SS	<foaf__age><wsdbm__friendOf>	1969	0.39
	SS	<foaf__age><dc__Location>	1948	0.39
	SS	<foaf__age><wsdbm__gender>	3053	0.6
	SS	<foaf__age><foaf__givenName>	3524	0.7
------
