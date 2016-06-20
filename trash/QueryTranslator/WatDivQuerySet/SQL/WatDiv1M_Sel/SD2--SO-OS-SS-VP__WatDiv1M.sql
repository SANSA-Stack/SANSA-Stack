SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__email$$1$$
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__faxNumber$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)


++++++Tables Statistic
sorg__email$$1$$	1	SS	sorg__email/sorg__faxNumber
	VP	<sorg__email>	9087
	SS	<sorg__email><sorg__faxNumber>	10	0.0
------
sorg__faxNumber$$2$$	1	SS	sorg__faxNumber/sorg__email
	VP	<sorg__faxNumber>	11
	SS	<sorg__faxNumber><sorg__email>	10	0.91
------
