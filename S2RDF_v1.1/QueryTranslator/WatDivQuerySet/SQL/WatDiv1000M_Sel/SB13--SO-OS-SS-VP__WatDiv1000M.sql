SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__author$$1$$
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM wsdbm__friendOf$$2$$
	
	) tab1
 ON(tab0.v1=tab1.v1)


++++++Tables Statistic
wsdbm__friendOf$$2$$	1	SO	wsdbm__friendOf/sorg__author
	VP	<wsdbm__friendOf>	449969341
	SO	<wsdbm__friendOf><sorg__author>	16939400	0.04
------
sorg__author$$1$$	1	OS	sorg__author/wsdbm__friendOf
	VP	<sorg__author>	399974
	OS	<sorg__author><wsdbm__friendOf>	158561	0.4
------
