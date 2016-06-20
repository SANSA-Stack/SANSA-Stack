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
	VP	<dc__Location>	40297
	SS	<dc__Location><wsdbm__likes>	9586	0.24
	SS	<dc__Location><wsdbm__friendOf>	16090	0.4
	SS	<dc__Location><foaf__age>	20247	0.5
	SS	<dc__Location><wsdbm__gender>	24028	0.6
	SS	<dc__Location><foaf__givenName>	28210	0.7
------
foaf__givenName$$6$$	1	SS	foaf__givenName/wsdbm__likes
	VP	<foaf__givenName>	69970
	SS	<foaf__givenName><wsdbm__likes>	16605	0.24
	SS	<foaf__givenName><wsdbm__friendOf>	27824	0.4
	SS	<foaf__givenName><dc__Location>	28210	0.4
	SS	<foaf__givenName><foaf__age>	35203	0.5
	SS	<foaf__givenName><wsdbm__gender>	41857	0.6
------
wsdbm__likes$$1$$	2	SS	wsdbm__likes/dc__Location
	VP	<wsdbm__likes>	112401
	SS	<wsdbm__likes><wsdbm__friendOf>	45187	0.4
	SS	<wsdbm__likes><dc__Location>	44994	0.4
	SS	<wsdbm__likes><foaf__age>	56266	0.5
	SS	<wsdbm__likes><wsdbm__gender>	67545	0.6
	SS	<wsdbm__likes><foaf__givenName>	78191	0.7
------
wsdbm__gender$$5$$	1	SS	wsdbm__gender/wsdbm__likes
	VP	<wsdbm__gender>	59784
	SS	<wsdbm__gender><wsdbm__likes>	14288	0.24
	SS	<wsdbm__gender><wsdbm__friendOf>	23833	0.4
	SS	<wsdbm__gender><dc__Location>	24028	0.4
	SS	<wsdbm__gender><foaf__age>	29826	0.5
	SS	<wsdbm__gender><foaf__givenName>	41857	0.7
------
wsdbm__friendOf$$2$$	1	SS	wsdbm__friendOf/wsdbm__likes
	VP	<wsdbm__friendOf>	4491142
	SS	<wsdbm__friendOf><wsdbm__likes>	1073954	0.24
	SS	<wsdbm__friendOf><dc__Location>	1819350	0.41
	SS	<wsdbm__friendOf><foaf__age>	2259754	0.5
	SS	<wsdbm__friendOf><wsdbm__gender>	2689820	0.6
	SS	<wsdbm__friendOf><foaf__givenName>	3139902	0.7
------
foaf__age$$4$$	1	SS	foaf__age/wsdbm__likes
	VP	<foaf__age>	50095
	SS	<foaf__age><wsdbm__likes>	11971	0.24
	SS	<foaf__age><wsdbm__friendOf>	19954	0.4
	SS	<foaf__age><dc__Location>	20247	0.4
	SS	<foaf__age><wsdbm__gender>	29826	0.6
	SS	<foaf__age><foaf__givenName>	35203	0.7
------
