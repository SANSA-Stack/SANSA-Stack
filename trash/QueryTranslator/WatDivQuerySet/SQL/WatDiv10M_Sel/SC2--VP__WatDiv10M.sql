SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 , obj AS v2 
	 FROM wsdbm__follows$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__friendOf$$1$$
	
	) tab0
 ON(tab1.v0=tab0.v0)


++++++Tables Statistic
wsdbm__follows$$2$$	0	VP	wsdbm__follows/
	VP	<wsdbm__follows>	3289307
------
wsdbm__friendOf$$1$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	4491142
------
