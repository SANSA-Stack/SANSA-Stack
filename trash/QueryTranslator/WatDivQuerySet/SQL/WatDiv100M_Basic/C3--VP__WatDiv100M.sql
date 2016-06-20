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
dc__Location$$3$$	0	VP	dc__Location/
	VP	<dc__Location>	400344
------
foaf__givenName$$6$$	0	VP	foaf__givenName/
	VP	<foaf__givenName>	700284
------
wsdbm__likes$$1$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	1124672
------
wsdbm__gender$$5$$	0	VP	wsdbm__gender/
	VP	<wsdbm__gender>	600332
------
wsdbm__friendOf$$2$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	45092208
------
foaf__age$$4$$	0	VP	foaf__age/
	VP	<foaf__age>	500284
------
