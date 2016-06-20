SELECT tab0.v1 AS v1 , tab2.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM gn__parentCountry$$1$$
	 
	 WHERE sub = 'wsdbm:City144'
	) tab0
 JOIN    (SELECT obj AS v1 , sub AS v2 
	 FROM sorg__nationality$$3$$
	
	) tab2
 ON(tab0.v1=tab2.v1)
 JOIN    (SELECT sub AS v2 
	 FROM wsdbm__likes$$2$$ 
	 WHERE obj = 'wsdbm:Product0'
	) tab1
 ON(tab2.v2=tab1.v2)


++++++Tables Statistic
gn__parentCountry$$1$$	0	VP	gn__parentCountry/
	VP	<gn__parentCountry>	240
------
wsdbm__likes$$2$$	1	SS	wsdbm__likes/sorg__nationality
	VP	<wsdbm__likes>	11256
	SS	<wsdbm__likes><sorg__nationality>	2089	0.19
------
sorg__nationality$$3$$	1	SS	sorg__nationality/wsdbm__likes
	VP	<sorg__nationality>	1957
	SS	<sorg__nationality><wsdbm__likes>	443	0.23
------
