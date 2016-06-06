SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM sorg__contactPoint$$1$$
	 
	 WHERE sub = 'wsdbm:Retailer18'
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
wsdbm__friendOf$$3$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	448135
------
wsdbm__follows$$2$$	0	VP	wsdbm__follows/
	VP	<wsdbm__follows>	330403
------
sorg__contactPoint$$1$$	0	VP	sorg__contactPoint/
	VP	<sorg__contactPoint>	99
------
gn__parentCountry$$5$$	0	VP	gn__parentCountry/
	VP	<gn__parentCountry>	240
------
dc__Location$$4$$	0	VP	dc__Location/
	VP	<dc__Location>	3925
------
