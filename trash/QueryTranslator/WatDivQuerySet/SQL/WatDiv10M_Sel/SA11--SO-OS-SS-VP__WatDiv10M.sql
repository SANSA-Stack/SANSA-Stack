SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v1 , obj AS v2 
	 FROM sorg__email$$2$$
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM wsdbm__friendOf$$1$$
	
	) tab0
 ON(tab1.v1=tab0.v1)


++++++Tables Statistic
wsdbm__friendOf$$1$$	1	OS	wsdbm__friendOf/sorg__email
	VP	<wsdbm__friendOf>	4491142
	OS	<wsdbm__friendOf><sorg__email>	4043831	0.9
------
sorg__email$$2$$	1	SO	sorg__email/wsdbm__friendOf
	VP	<sorg__email>	91004
	SO	<sorg__email><wsdbm__friendOf>	90068	0.99
------
