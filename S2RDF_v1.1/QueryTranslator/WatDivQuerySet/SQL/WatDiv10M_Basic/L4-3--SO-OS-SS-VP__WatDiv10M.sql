SELECT tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM og__tag$$1$$ 
	 WHERE obj = 'wsdbm:Topic216'
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__caption$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
sorg__caption$$2$$	1	SS	sorg__caption/og__tag
	VP	<sorg__caption>	2501
	SS	<sorg__caption><og__tag>	1505	0.6
------
og__tag$$1$$	1	SS	og__tag/sorg__caption
	VP	<og__tag>	147271
	SS	<og__tag><sorg__caption>	14662	0.1
------
