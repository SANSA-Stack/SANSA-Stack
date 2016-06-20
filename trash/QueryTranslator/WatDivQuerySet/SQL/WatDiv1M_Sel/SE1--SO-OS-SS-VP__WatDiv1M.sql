SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab2.v3 AS v3 , tab2.v2 AS v2 
 FROM    (SELECT obj AS v3 , sub AS v2 
	 FROM foaf__homepage$$3$$
	
	) tab2
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__follows$$2$$
	
	) tab1
 ON(tab2.v2=tab1.v2)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__friendOf$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
wsdbm__follows$$2$$	2	OS	wsdbm__follows/foaf__homepage
	VP	<wsdbm__follows>	330403
	SO	<wsdbm__follows><wsdbm__friendOf>	330403	1.0
	OS	<wsdbm__follows><foaf__homepage>	14952	0.05
------
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/wsdbm__follows
	VP	<wsdbm__friendOf>	448135
	OS	<wsdbm__friendOf><wsdbm__follows>	346685	0.77
------
foaf__homepage$$3$$	1	SO	foaf__homepage/wsdbm__follows
	VP	<foaf__homepage>	1068
	SO	<foaf__homepage><wsdbm__follows>	441	0.41
------
