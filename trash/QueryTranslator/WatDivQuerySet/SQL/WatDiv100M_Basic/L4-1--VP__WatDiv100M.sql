SELECT tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM og__tag$$1$$ 
	 WHERE obj = 'wsdbm:Topic142'
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__caption$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
sorg__caption$$2$$	0	VP	sorg__caption/
	VP	<sorg__caption>	24836
------
og__tag$$1$$	0	VP	og__tag/
	VP	<og__tag>	1500803
------
