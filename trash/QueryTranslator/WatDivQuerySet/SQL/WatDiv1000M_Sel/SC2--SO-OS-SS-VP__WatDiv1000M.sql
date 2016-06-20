SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 , obj AS v2 
	 FROM wsdbm__follows$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__friendOf$$1$$
	
	) tab0
 ON(tab1.v0=tab0.v0)


++++++Tables Statistic
wsdbm__follows$$2$$	1	SS	wsdbm__follows/wsdbm__friendOf
	VP	<wsdbm__follows>	327487530
	SS	<wsdbm__follows><wsdbm__friendOf>	130814349	0.4
------
wsdbm__friendOf$$1$$	1	SS	wsdbm__friendOf/wsdbm__follows
	VP	<wsdbm__friendOf>	449969341
	SS	<wsdbm__friendOf><wsdbm__follows>	348581107	0.77
------
