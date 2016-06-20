SELECT tab2.subject AS subject , tab0.name AS name , tab2.prize AS prize , tab1.famName AS famName 
 FROM    (SELECT sub AS subject , obj AS prize 
	 FROM _L_hasWonPrize_B_$$3$$
	
	) tab2
 JOIN    (SELECT sub AS subject , obj AS name 
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
_L_hasWonPrize_B_$$3$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
------
