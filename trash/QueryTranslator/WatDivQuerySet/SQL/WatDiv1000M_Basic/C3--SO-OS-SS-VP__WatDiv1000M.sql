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
	VP	<dc__Location>	4000049
	SS	<dc__Location><wsdbm__likes>	953644	0.24
	SS	<dc__Location><wsdbm__friendOf>	1597823	0.4
	SS	<dc__Location><foaf__age>	1998778	0.5
	SS	<dc__Location><wsdbm__gender>	2399839	0.6
	SS	<dc__Location><foaf__givenName>	2800507	0.7
------
foaf__givenName$$6$$	1	SS	foaf__givenName/wsdbm__likes
	VP	<foaf__givenName>	7001593
	SS	<foaf__givenName><wsdbm__likes>	1667157	0.24
	SS	<foaf__givenName><wsdbm__friendOf>	2796040	0.4
	SS	<foaf__givenName><dc__Location>	2800507	0.4
	SS	<foaf__givenName><foaf__age>	3499830	0.5
	SS	<foaf__givenName><wsdbm__gender>	4200061	0.6
------
wsdbm__likes$$1$$	1	SS	wsdbm__likes/wsdbm__friendOf
	VP	<wsdbm__likes>	11246476
	SS	<wsdbm__likes><wsdbm__friendOf>	4491944	0.4
	SS	<wsdbm__likes><dc__Location>	4499071	0.4
	SS	<wsdbm__likes><foaf__age>	5618468	0.5
	SS	<wsdbm__likes><wsdbm__gender>	6747200	0.6
	SS	<wsdbm__likes><foaf__givenName>	7876012	0.7
------
wsdbm__gender$$5$$	1	SS	wsdbm__gender/wsdbm__likes
	VP	<wsdbm__gender>	5999423
	SS	<wsdbm__gender><wsdbm__likes>	1429329	0.24
	SS	<wsdbm__gender><wsdbm__friendOf>	2396420	0.4
	SS	<wsdbm__gender><dc__Location>	2399839	0.4
	SS	<wsdbm__gender><foaf__age>	2998496	0.5
	SS	<wsdbm__gender><foaf__givenName>	4200061	0.7
------
wsdbm__friendOf$$2$$	1	SS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	449969341
	SS	<wsdbm__friendOf><wsdbm__likes>	107041459	0.24
	SS	<wsdbm__friendOf><dc__Location>	179979848	0.4
	SS	<wsdbm__friendOf><foaf__age>	224953865	0.5
	SS	<wsdbm__friendOf><wsdbm__gender>	269989075	0.6
	SS	<wsdbm__friendOf><foaf__givenName>	315013904	0.7
------
foaf__age$$4$$	1	SS	foaf__age/wsdbm__likes
	VP	<foaf__age>	4998434
	SS	<foaf__age><wsdbm__likes>	1190991	0.24
	SS	<foaf__age><wsdbm__friendOf>	1996355	0.4
	SS	<foaf__age><dc__Location>	1998778	0.4
	SS	<foaf__age><wsdbm__gender>	2998496	0.6
	SS	<foaf__age><foaf__givenName>	3499830	0.7
------
