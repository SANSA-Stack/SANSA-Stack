SELECT tab0.v1 AS v1 , tab1.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__faxNumber$$2$$
	
	) tab1
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__email$$1$$
	) tab0
 ON(tab1.v0=tab0.v0)


++++++Tables Statistic
sorg__email$$1$$	0	VP	sorg__email/
	VP	<sorg__email>	9096281
------
sorg__faxNumber$$2$$	0	VP	sorg__faxNumber/
	VP	<sorg__faxNumber>	11970
------
