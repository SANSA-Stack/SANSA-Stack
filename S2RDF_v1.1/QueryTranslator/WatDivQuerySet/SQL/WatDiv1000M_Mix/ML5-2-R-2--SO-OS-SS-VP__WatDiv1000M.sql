SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM sorg__contactPoint$$1$$
	 
	 WHERE sub = 'wsdbm:Retailer16582'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__follows$$2$$
	
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM wsdbm__friendOf$$3$$
	
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM dc__Location$$4$$
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM gn__parentCountry$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)


++++++Tables Statistic
wsdbm__friendOf$$3$$	2	OS	wsdbm__friendOf/dc__Location
	VP	<wsdbm__friendOf>	449969341
	SO	<wsdbm__friendOf><wsdbm__follows>	405358991	0.9
	OS	<wsdbm__friendOf><dc__Location>	180001194	0.4
------
wsdbm__follows$$2$$	1	SO	wsdbm__follows/sorg__contactPoint
	VP	<wsdbm__follows>	327487530
	SO	<wsdbm__follows><sorg__contactPoint>	3137902	0.01
	OS	<wsdbm__follows><wsdbm__friendOf>	130704139	0.4
------
sorg__contactPoint$$1$$	1	OS	sorg__contactPoint/wsdbm__follows
	VP	<sorg__contactPoint>	95822
	OS	<sorg__contactPoint><wsdbm__follows>	74095	0.77
------
gn__parentCountry$$5$$	0	VP	gn__parentCountry/
	VP	<gn__parentCountry>	240
	SO	<gn__parentCountry><dc__Location>	240	1.0
------
dc__Location$$4$$	0	VP	dc__Location/
	VP	<dc__Location>	4000049
	SO	<dc__Location><wsdbm__friendOf>	4000049	1.0
	OS	<dc__Location><gn__parentCountry>	4000049	1.0
------
