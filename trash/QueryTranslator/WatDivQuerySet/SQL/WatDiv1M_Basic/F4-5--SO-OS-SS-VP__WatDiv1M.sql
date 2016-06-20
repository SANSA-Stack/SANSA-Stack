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
	 WHERE obj = 'wsdbm:Topic106'
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
	VP	<sorg__url>	500
	SO	<sorg__url><foaf__homepage>	493	0.99
	SS	<sorg__url><wsdbm__hits>	500	1.0
	SS	<sorg__url><sorg__language>	500	1.0
------
sorg__contentSize$$5$$	1	SS	sorg__contentSize/foaf__homepage
	VP	<sorg__contentSize>	245
	SS	<sorg__contentSize><foaf__homepage>	43	0.18
	SO	<sorg__contentSize><gr__includes>	239	0.98
	SS	<sorg__contentSize><og__tag>	147	0.6
	SS	<sorg__contentSize><sorg__description>	164	0.67
	SO	<sorg__contentSize><wsdbm__likes>	231	0.94
------
wsdbm__likes$$9$$	4	OS	wsdbm__likes/sorg__contentSize
	VP	<wsdbm__likes>	11256
	OS	<wsdbm__likes><foaf__homepage>	2677	0.24
	OS	<wsdbm__likes><og__tag>	7270	0.65
	OS	<wsdbm__likes><sorg__description>	7592	0.67
	OS	<wsdbm__likes><sorg__contentSize>	996	0.09
------
og__tag$$3$$	4	SS	og__tag/sorg__contentSize
	VP	<og__tag>	15121
	SS	<og__tag><foaf__homepage>	3490	0.23
	SO	<og__tag><gr__includes>	14341	0.95
	SS	<og__tag><sorg__description>	9096	0.6
	SS	<og__tag><sorg__contentSize>	1421	0.09
	SO	<og__tag><wsdbm__likes>	14022	0.93
------
sorg__language$$8$$	1	SO	sorg__language/foaf__homepage
	VP	<sorg__language>	655
	SO	<sorg__language><foaf__homepage>	493	0.75
	SS	<sorg__language><sorg__url>	500	0.76
	SS	<sorg__language><wsdbm__hits>	500	0.76
------
gr__includes$$2$$	4	OS	gr__includes/sorg__contentSize
	VP	<gr__includes>	9000
	OS	<gr__includes><foaf__homepage>	2137	0.24
	OS	<gr__includes><og__tag>	5408	0.6
	OS	<gr__includes><sorg__description>	5540	0.62
	OS	<gr__includes><sorg__contentSize>	863	0.1
------
wsdbm__hits$$7$$	1	SO	wsdbm__hits/foaf__homepage
	VP	<wsdbm__hits>	500
	SO	<wsdbm__hits><foaf__homepage>	493	0.99
	SS	<wsdbm__hits><sorg__url>	500	1.0
	SS	<wsdbm__hits><sorg__language>	500	1.0
------
sorg__description$$4$$	4	SS	sorg__description/sorg__contentSize
	VP	<sorg__description>	1534
	SS	<sorg__description><foaf__homepage>	362	0.24
	SO	<sorg__description><gr__includes>	1496	0.98
	SS	<sorg__description><og__tag>	936	0.61
	SS	<sorg__description><sorg__contentSize>	164	0.11
	SO	<sorg__description><wsdbm__likes>	1463	0.95
------
foaf__homepage$$1$$	4	SS	foaf__homepage/sorg__contentSize
	VP	<foaf__homepage>	1068
	SO	<foaf__homepage><gr__includes>	575	0.54
	SS	<foaf__homepage><og__tag>	347	0.32
	SS	<foaf__homepage><sorg__description>	362	0.34
	SS	<foaf__homepage><sorg__contentSize>	43	0.04
	OS	<foaf__homepage><sorg__url>	1068	1.0
	OS	<foaf__homepage><wsdbm__hits>	1068	1.0
	OS	<foaf__homepage><sorg__language>	1068	1.0
	SO	<foaf__homepage><wsdbm__likes>	553	0.52
------
