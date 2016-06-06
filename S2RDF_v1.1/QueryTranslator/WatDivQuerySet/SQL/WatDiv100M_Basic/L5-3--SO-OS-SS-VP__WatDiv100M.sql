SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab1.v3 AS v3 
 FROM    (SELECT obj AS v3 
	 FROM gn__parentCountry$$2$$
	 
	 WHERE sub = 'wsdbm:City158'
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
sorg__nationality$$3$$	1	SS	sorg__nationality/sorg__jobTitle
	VP	<sorg__nationality>	200780
	SS	<sorg__nationality><sorg__jobTitle>	10047	0.05
------
sorg__jobTitle$$1$$	1	SS	sorg__jobTitle/sorg__nationality
	VP	<sorg__jobTitle>	50256
	SS	<sorg__jobTitle><sorg__nationality>	10047	0.2
------
