SELECT tab7.v1 AS v1 , tab0.v0 AS v0 , tab8.v7 AS v7 , tab5.v5 AS v5 , tab3.v4 AS v4 , tab6.v6 AS v6 , tab1.v2 AS v2 , tab4.v8 AS v8 
 FROM    (SELECT sub AS v1 
	 FROM sorg__language$$8$$
	 
	 WHERE obj = 'wsdbm:Language0'
	) tab7
 JOIN    (SELECT sub AS v1 , obj AS v5 
	 FROM sorg__url$$6$$
	) tab5
 ON(tab7.v1=tab5.v1)
 JOIN    (SELECT sub AS v1 , obj AS v6 
	 FROM wsdbm__hits$$7$$
	) tab6
 ON(tab5.v1=tab6.v1)
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM foaf__homepage$$1$$
	
	) tab0
 ON(tab6.v1=tab0.v1)
 JOIN    (SELECT sub AS v0 
	 FROM og__tag$$3$$ 
	 WHERE obj = 'wsdbm:Topic76'
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
 JOIN    (SELECT obj AS v0 , sub AS v2 
	 FROM gr__includes$$2$$
	) tab1
 ON(tab3.v0=tab1.v0)
 JOIN    (SELECT obj AS v0 , sub AS v7 
	 FROM wsdbm__likes$$9$$
	) tab8
 ON(tab1.v0=tab8.v0)


++++++Tables Statistic
sorg__url$$6$$	0	VP	sorg__url/
	VP	<sorg__url>	500000
------
sorg__contentSize$$5$$	0	VP	sorg__contentSize/
	VP	<sorg__contentSize>	249962
------
wsdbm__likes$$9$$	0	VP	wsdbm__likes/
	VP	<wsdbm__likes>	11246476
------
og__tag$$3$$	0	VP	og__tag/
	VP	<og__tag>	14987949
------
sorg__language$$8$$	0	VP	sorg__language/
	VP	<sorg__language>	641195
------
gr__includes$$2$$	0	VP	gr__includes/
	VP	<gr__includes>	9000000
------
wsdbm__hits$$7$$	0	VP	wsdbm__hits/
	VP	<wsdbm__hits>	500000
------
sorg__description$$4$$	0	VP	sorg__description/
	VP	<sorg__description>	1502748
------
foaf__homepage$$1$$	0	VP	foaf__homepage/
	VP	<foaf__homepage>	1118496
------
