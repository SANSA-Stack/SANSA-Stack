SELECT tab0.person AS person , tab2.subject AS subject , tab1.famName AS famName , tab2.known AS known 
 FROM    (SELECT sub AS subject , obj AS known 
	 FROM _L_isKnownFor_B_$$3$$
	) tab2
 JOIN    (SELECT obj AS person , sub AS subject 
	 FROM _L_hasGivenName_B_$$1$$
	
	) tab0
 ON(tab2.subject=tab0.subject)
 JOIN    (SELECT sub AS subject , obj AS famName 
	 FROM _L_hasFamilyName_B_$$2$$
	
	) tab1
 ON(tab0.subject=tab1.subject)


++++++Tables Statistic
_L_hasGivenName_B_$$1$$	0	VP	_L_hasGivenName_B_/
	VP	<hasGivenName>	827681
------
_L_hasFamilyName_B_$$2$$	0	VP	_L_hasFamilyName_B_/
	VP	<hasFamilyName>	838669
------
_L_isKnownFor_B_$$3$$	0	VP	_L_isKnownFor_B_/
	VP	<isKnownFor>	500
------
