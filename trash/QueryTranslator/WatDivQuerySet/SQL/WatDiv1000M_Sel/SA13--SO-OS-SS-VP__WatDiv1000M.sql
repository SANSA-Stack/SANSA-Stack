SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM sorg__jobTitle$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__friendOf$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/sorg__jobTitle
	VP	<wsdbm__friendOf>	449969341
	OS	<wsdbm__friendOf><sorg__jobTitle>	22471302	0.05
------
sorg__jobTitle$$2$$	0	VP	sorg__jobTitle/
	VP	<sorg__jobTitle>	499527
	SO	<sorg__jobTitle><wsdbm__friendOf>	499527	1.0
------
