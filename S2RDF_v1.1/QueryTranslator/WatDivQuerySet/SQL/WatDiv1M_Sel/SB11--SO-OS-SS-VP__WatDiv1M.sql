SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__follows$$1$$
	
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__friendOf$$2$$
	
	) tab1
 ON(tab0.v1=tab1.v1)


++++++Tables Statistic
wsdbm__friendOf$$2$$	1	SO	wsdbm__friendOf/wsdbm__follows
	VP	<wsdbm__friendOf>	448135
	SO	<wsdbm__friendOf><wsdbm__follows>	413110	0.92
------
wsdbm__follows$$1$$	1	OS	wsdbm__follows/wsdbm__friendOf
	VP	<wsdbm__follows>	330403
	OS	<wsdbm__follows><wsdbm__friendOf>	130705	0.4
------
