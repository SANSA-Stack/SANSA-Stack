SELECT tab0.subject AS subject , tab2.prize AS prize , tab0.name AS name , tab1.famName AS famName 
 FROM    (SELECT sub AS subject , obj AS name 
	 FROM _L_hasGivenName_B_$$1$$
	
	) tab0
 JOIN    (SELECT sub AS subject , obj AS famName 
	 FROM _L_hasFamilyName_B_$$2$$
	
	) tab1
 ON(tab0.subject=tab1.subject)
 JOIN    (SELECT sub AS subject , obj AS prize 
	 FROM _L_hasWonPrize_B_$$3$$
	
	) tab2
 ON(tab1.subject=tab2.subject)


++++++Tables Statistic
_L_hasGivenName_B_$$1$$	2	SS	_L_hasGivenName_B_/_L_hasWonPrize_B_
	VP	<hasGivenName>	827681
	SS	<hasGivenName><hasFamilyName>	827681	1.0
	SS	<hasGivenName><hasWonPrize>	37549	0.05
------
_L_hasFamilyName_B_$$2$$	2	SS	_L_hasFamilyName_B_/_L_hasWonPrize_B_
	VP	<hasFamilyName>	838669
	SS	<hasFamilyName><hasGivenName>	827681	0.99
	SS	<hasFamilyName><hasWonPrize>	38247	0.05
------
_L_hasWonPrize_B_$$3$$	1	SS	_L_hasWonPrize_B_/_L_hasGivenName_B_
	VP	<hasWonPrize>	73763
	SS	<hasWonPrize><hasGivenName>	64719	0.88
	SS	<hasWonPrize><hasFamilyName>	65862	0.89
------
