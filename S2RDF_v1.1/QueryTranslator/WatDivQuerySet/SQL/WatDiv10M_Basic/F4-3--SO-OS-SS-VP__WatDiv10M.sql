SELECT tab7.v1 AS v1 , tab0.v0 AS v0 , tab8.v7 AS v7 , tab5.v5 AS v5 , tab6.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2 , tab4.v8 AS v8 
 FROM    (SELECT sub AS v1 
	 FROM sorg__language$$8$$
	 
	 WHERE obj = 'wsdbm:Language0'
	) tab7
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM foaf__homepage$$1$$
	
	) tab0
 ON(tab7.v1=tab0.v1)
 JOIN    (SELECT sub AS v0 
	 FROM og__tag$$3$$ 
	 WHERE obj = 'wsdbm:Topic71'
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v8 
	 FROM sorg__contentSize$$5$$
	
	) tab4
 ON(tab2.v0=tab4.v0)
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM sorg__description$$4$$
	
	) tab3
 ON(tab4.v0=tab3.v0)
 JOIN    (SELECT sub AS v1 , obj AS v5 
	 FROM sorg__url$$6$$
	) tab5
 ON(tab0.v1=tab5.v1)
 JOIN    (SELECT sub AS v1 , obj AS v6 
	 FROM wsdbm__hits$$7$$
	) tab6
 ON(tab5.v1=tab6.v1)
 JOIN    (SELECT obj AS v0 , sub AS v2 
	 FROM gr__includes$$2$$
	) tab1
 ON(tab3.v0=tab1.v0)
 JOIN    (SELECT obj AS v0 , sub AS v7 
	 FROM wsdbm__likes$$9$$
	) tab8
 ON(tab1.v0=tab8.v0)


++++++Tables Statistic
sorg__url$$6$$	1	SO	sorg__url/foaf__homepage
	VP	<sorg__url>	5000
	SO	<sorg__url><foaf__homepage>	4983	1.0
	SS	<sorg__url><wsdbm__hits>	5000	1.0
	SS	<sorg__url><sorg__language>	5000	1.0
------
sorg__contentSize$$5$$	1	SS	sorg__contentSize/foaf__homepage
	VP	<sorg__contentSize>	2438
	SS	<sorg__contentSize><foaf__homepage>	616	0.25
	SO	<sorg__contentSize><gr__includes>	2360	0.97
	SS	<sorg__contentSize><og__tag>	1436	0.59
	SS	<sorg__contentSize><sorg__description>	1448	0.59
	SO	<sorg__contentSize><wsdbm__likes>	2330	0.96
------
wsdbm__likes$$9$$	4	OS	wsdbm__likes/sorg__contentSize
	VP	<wsdbm__likes>	112401
	OS	<wsdbm__likes><foaf__homepage>	26377	0.23
	OS	<wsdbm__likes><og__tag>	64239	0.57
	OS	<wsdbm__likes><sorg__description>	69713	0.62
	OS	<wsdbm__likes><sorg__contentSize>	10190	0.09
------
og__tag$$3$$	4	SS	og__tag/sorg__contentSize
	VP	<og__tag>	147271
	SS	<og__tag><foaf__homepage>	36952	0.25
	SO	<og__tag><gr__includes>	142240	0.97
	SS	<og__tag><sorg__description>	87737	0.6
	SS	<og__tag><sorg__contentSize>	14270	0.1
	SO	<og__tag><wsdbm__likes>	140021	0.95
------
sorg__language$$8$$	1	SO	sorg__language/foaf__homepage
	VP	<sorg__language>	6251
	SO	<sorg__language><foaf__homepage>	4983	0.8
	SS	<sorg__language><sorg__url>	5000	0.8
	SS	<sorg__language><wsdbm__hits>	5000	0.8
------
gr__includes$$2$$	4	OS	gr__includes/sorg__contentSize
	VP	<gr__includes>	90000
	OS	<gr__includes><foaf__homepage>	22529	0.25
	OS	<gr__includes><og__tag>	53879	0.6
	OS	<gr__includes><sorg__description>	53801	0.6
	OS	<gr__includes><sorg__contentSize>	8719	0.1
------
wsdbm__hits$$7$$	1	SO	wsdbm__hits/foaf__homepage
	VP	<wsdbm__hits>	5000
	SO	<wsdbm__hits><foaf__homepage>	4983	1.0
	SS	<wsdbm__hits><sorg__url>	5000	1.0
	SS	<wsdbm__hits><sorg__language>	5000	1.0
------
sorg__description$$4$$	4	SS	sorg__description/sorg__contentSize
	VP	<sorg__description>	14960
	SS	<sorg__description><foaf__homepage>	3760	0.25
	SO	<sorg__description><gr__includes>	14532	0.97
	SS	<sorg__description><og__tag>	8948	0.6
	SS	<sorg__description><sorg__contentSize>	1448	0.1
	SO	<sorg__description><wsdbm__likes>	14248	0.95
------
foaf__homepage$$1$$	4	SS	foaf__homepage/sorg__contentSize
	VP	<foaf__homepage>	11204
	SO	<foaf__homepage><gr__includes>	6095	0.54
	SS	<foaf__homepage><og__tag>	3739	0.33
	SS	<foaf__homepage><sorg__description>	3760	0.34
	SS	<foaf__homepage><sorg__contentSize>	616	0.05
	OS	<foaf__homepage><sorg__url>	11204	1.0
	OS	<foaf__homepage><wsdbm__hits>	11204	1.0
	OS	<foaf__homepage><sorg__language>	11204	1.0
	SO	<foaf__homepage><wsdbm__likes>	5961	0.53
------
