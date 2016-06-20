SELECT tab3.v0 AS v0 , tab2.v3 AS v3 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM sorg__language$$4$$
	 
	 WHERE obj = 'wsdbm:Language0'
	) tab3
 JOIN    (SELECT sub AS v0 
	 FROM rdf__type$$1$$ 
	 WHERE obj = 'wsdbm:ProductCategory5'
	) tab0
 ON(tab3.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM sorg__keywords$$3$$
	
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__description$$2$$
	
	) tab1
 ON(tab2.v0=tab1.v0)


++++++Tables Statistic
sorg__description$$2$$	3	SS	sorg__description/sorg__language
	VP	<sorg__description>	14960
	SS	<sorg__description><rdf__type>	14960	1.0
	SS	<sorg__description><sorg__keywords>	4459	0.3
	SS	<sorg__description><sorg__language>	310	0.02
------
rdf__type$$1$$	3	SS	rdf__type/sorg__language
	VP	<rdf__type>	136215
	SS	<rdf__type><sorg__description>	14960	0.11
	SS	<rdf__type><sorg__keywords>	7410	0.05
	SS	<rdf__type><sorg__language>	514	0.0
------
sorg__keywords$$3$$	3	SS	sorg__keywords/sorg__language
	VP	<sorg__keywords>	7410
	SS	<sorg__keywords><rdf__type>	7410	1.0
	SS	<sorg__keywords><sorg__description>	4459	0.6
	SS	<sorg__keywords><sorg__language>	142	0.02
------
sorg__language$$4$$	3	SS	sorg__language/sorg__keywords
	VP	<sorg__language>	6251
	SS	<sorg__language><rdf__type>	1251	0.2
	SS	<sorg__language><sorg__description>	774	0.12
	SS	<sorg__language><sorg__keywords>	365	0.06
------
