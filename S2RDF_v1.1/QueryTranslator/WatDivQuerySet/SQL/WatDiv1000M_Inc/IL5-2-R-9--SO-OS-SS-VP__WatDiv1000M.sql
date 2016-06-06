SELECT tab0.v1 AS v1 , tab4.v5 AS v5 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 
	 FROM gr__offers$$1$$ 
	 WHERE sub = 'wsdbm:Retailer17546'
	) tab0
 JOIN    (SELECT sub AS v1 , obj AS v2 
	 FROM gr__includes$$2$$
	) tab1
 ON(tab0.v1=tab1.v1)
 JOIN    (SELECT obj AS v3 , sub AS v2 
	 FROM sorg__director$$3$$
	
	) tab2
 ON(tab1.v2=tab2.v2)
 JOIN    (SELECT obj AS v4 , sub AS v3 
	 FROM wsdbm__friendOf$$4$$
	
	) tab3
 ON(tab2.v3=tab3.v3)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__friendOf$$5$$
	
	) tab4
 ON(tab3.v4=tab4.v4)


++++++Tables Statistic
sorg__director$$3$$	2	OS	sorg__director/wsdbm__friendOf
	VP	<sorg__director>	133767
	SO	<sorg__director><gr__includes>	130128	0.97
	OS	<sorg__director><wsdbm__friendOf>	50992	0.38
------
gr__offers$$1$$	0	VP	gr__offers/
	VP	<gr__offers>	14156906
	OS	<gr__offers><gr__includes>	14156906	1.0
------
wsdbm__friendOf$$4$$	1	SO	wsdbm__friendOf/sorg__director
	VP	<wsdbm__friendOf>	449969341
	SO	<wsdbm__friendOf><sorg__director>	2334445	0.01
	OS	<wsdbm__friendOf><wsdbm__friendOf>	179722774	0.4
------
gr__includes$$2$$	2	OS	gr__includes/sorg__director
	VP	<gr__includes>	9000000
	SO	<gr__includes><gr__offers>	4166185	0.46
	OS	<gr__includes><sorg__director>	481348	0.05
------
wsdbm__friendOf$$5$$	0	VP	wsdbm__friendOf/
	VP	<wsdbm__friendOf>	449969341
	SO	<wsdbm__friendOf><wsdbm__friendOf>	449969341	1.0
------
