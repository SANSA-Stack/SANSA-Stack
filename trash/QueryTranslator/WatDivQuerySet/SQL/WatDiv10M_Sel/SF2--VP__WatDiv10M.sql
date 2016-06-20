SELECT tab1.v1 AS v1 , tab0.v0 AS v0 , tab2.v3 AS v3 , tab2.v2 AS v2 
 FROM    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__language$$3$$
	
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
wsdbm__follows$$2$$	0	VP	wsdbm__follows/
	VP	<wsdbm__follows>	3289307
------
wsdbm__friendOf$$1$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	4491142
------
sorg__language$$3$$	0	VP	sorg__language/
	VP	<sorg__language>	6251
------
