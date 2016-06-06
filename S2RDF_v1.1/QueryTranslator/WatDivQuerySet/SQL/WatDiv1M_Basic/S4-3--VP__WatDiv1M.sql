SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM sorg__nationality$$4$$
	 
	 WHERE obj = 'wsdbm:Country1'
	) tab3
 JOIN    (SELECT sub AS v0 
	 FROM foaf__age$$1$$ 
	 WHERE obj = 'wsdbm:AgeGroup5'
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT obj AS v0 , sub AS v3 
	 FROM mo__artist$$3$$
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM foaf__familyName$$2$$
	
	) tab1
 ON(tab2.v0=tab1.v0)


++++++Tables Statistic
foaf__familyName$$2$$	0	VP	foaf__familyName/
	VP	<foaf__familyName>	6968
------
mo__artist$$3$$	0	VP	mo__artist/
	VP	<mo__artist>	115
------
sorg__nationality$$4$$	0	VP	sorg__nationality/
	VP	<sorg__nationality>	1957
------
foaf__age$$1$$	0	VP	foaf__age/
	VP	<foaf__age>	5054
------
