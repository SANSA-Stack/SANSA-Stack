SELECT tab0.person AS person , tab0.subject AS subject , tab1.famName AS famName , tab2.known AS known 
 FROM    (SELECT obj AS person , sub AS subject 
	 FROM _L_hasGivenName_B_$$1$$
	
	) tab0
 JOIN    (SELECT sub AS subject , obj AS famName 
	 FROM _L_hasFamilyName_B_$$2$$
	
	) tab1
 ON(tab0.subject=tab1.subject)
 JOIN    (SELECT sub AS subject , obj AS known 
	 FROM _L_isKnownFor_B_$$3$$
	) tab2
 ON(tab1.subject=tab2.subject)


++++++Tables Statistic
_L_hasGivenName_B_$$1$$	2	SS	_L_hasGivenName_B_/_L_isKnownFor_B_
	VP	<hasGivenName>	827681
	SS	<hasGivenName><hasFamilyName>	827681	1.0
	SS	<hasGivenName><isKnownFor>	269	0.0
------
_L_hasFamilyName_B_$$2$$	2	SS	_L_hasFamilyName_B_/_L_isKnownFor_B_
	VP	<hasFamilyName>	838669
	SS	<hasFamilyName><hasGivenName>	827681	0.99
	SS	<hasFamilyName><isKnownFor>	277	0.0
------
_L_isKnownFor_B_$$3$$	1	SS	_L_isKnownFor_B_/_L_hasGivenName_B_
	VP	<isKnownFor>	500
	SS	<isKnownFor><hasGivenName>	431	0.86
	SS	<isKnownFor><hasFamilyName>	441	0.88
------
