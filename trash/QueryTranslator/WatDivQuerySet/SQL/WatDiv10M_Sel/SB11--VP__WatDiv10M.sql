SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__follows$$1$$
	
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__friendOf$$2$$
	
	) tab1
 ON(tab0.v1=tab1.v1)


++++++Tables Statistic
wsdbm__friendOf$$2$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	4491142
------
wsdbm__follows$$1$$	0	VP	wsdbm__follows/
	VP	<wsdbm__follows>	3289307
------
