SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v3 AS v3 
 FROM    (SELECT obj AS v3 
	 FROM gn__parentCountry$$2$$
	 
	 WHERE sub = 'wsdbm:City65'
	) tab1
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM sorg__nationality$$3$$
	
	) tab2
 ON(tab1.v3=tab2.v3)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__jobTitle$$1$$
	
	) tab0
 ON(tab2.v0=tab0.v0)


++++++Tables Statistic
gn__parentCountry$$2$$	0	VP	gn__parentCountry/
	VP	<gn__parentCountry>	240
------
sorg__nationality$$3$$	0	VP	sorg__nationality/
	VP	<sorg__nationality>	1957
------
sorg__jobTitle$$1$$	0	VP	sorg__jobTitle/
	VP	<sorg__jobTitle>	500
------
