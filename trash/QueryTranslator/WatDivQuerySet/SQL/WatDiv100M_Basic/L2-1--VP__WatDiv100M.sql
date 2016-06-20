SELECT tab0.v1 AS v1 , tab2.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM gn__parentCountry$$1$$
	 
	 WHERE sub = 'wsdbm:City70'
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
wsdbm__likes$$2$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	1124672
------
sorg__nationality$$3$$	0	VP	sorg__nationality/
	VP	<sorg__nationality>	200780
------
