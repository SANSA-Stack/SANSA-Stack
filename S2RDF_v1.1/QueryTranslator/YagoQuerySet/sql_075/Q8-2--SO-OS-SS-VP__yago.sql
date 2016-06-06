SELECT tab2.individual AS individual , tab5.prize AS prize , tab4.name AS name , tab3.musician AS musician 
 FROM    (SELECT sub AS musician 
	 FROM skos__prefLabel$$4$$
	 
	 WHERE obj = '"musician"@eng'
	) tab3
 JOIN    (SELECT sub AS individual , obj AS musician 
	 FROM rdf__type$$3$$
	) tab2
 ON(tab3.musician=tab2.musician)
 JOIN    (SELECT sub AS individual 
	 FROM rdf__type$$1$$ 
	 WHERE obj = '<wikicategory_American_male_singers>'
	) tab0
 ON(tab2.individual=tab0.individual)
 JOIN    (SELECT sub AS individual 
	 FROM rdf__type$$2$$ 
	 WHERE obj = '<wikicategory_Roadrunner_Records_artists>'
	) tab1
 ON(tab0.individual=tab1.individual)
 JOIN    (SELECT sub AS individual , obj AS name 
	 FROM skos__prefLabel$$5$$
	
	) tab4
 ON(tab1.individual=tab4.individual)
 JOIN    (SELECT sub AS individual , obj AS prize 
	 FROM _L_hasWonPrize_B_$$6$$
	
	) tab5
 ON(tab4.individual=tab5.individual)


++++++Tables Statistic
skos__prefLabel$$4$$	1	SO	skos__prefLabel/rdf__type
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><rdf__type>	8678	0.0
------
skos__prefLabel$$5$$	4	SS	skos__prefLabel/_L_hasWonPrize_B_
	VP	<skos__prefLabel>	2954875
	SS	<skos__prefLabel><rdf__type>	2886010	0.98
	SS	<skos__prefLabel><rdf__type>	2886010	0.98
	SS	<skos__prefLabel><rdf__type>	2886010	0.98
	SS	<skos__prefLabel><hasWonPrize>	43615	0.01
------
rdf__type$$2$$	4	SS	rdf__type/_L_hasWonPrize_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><skos__prefLabel>	61159360	1.0
	SS	<rdf__type><hasWonPrize>	1487073	0.02
------
rdf__type$$1$$	4	SS	rdf__type/_L_hasWonPrize_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><skos__prefLabel>	61159360	1.0
	SS	<rdf__type><hasWonPrize>	1487073	0.02
------
rdf__type$$3$$	5	SS	rdf__type/_L_hasWonPrize_B_
	VP	<rdf__type>	61165359
	SS	<rdf__type><rdf__type>	61165359	1.0
	SS	<rdf__type><rdf__type>	61165359	1.0
	OS	<rdf__type><skos__prefLabel>	42863266	0.7
	SS	<rdf__type><skos__prefLabel>	61159360	1.0
	SS	<rdf__type><hasWonPrize>	1487073	0.02
------
_L_hasWonPrize_B_$$6$$	0	VP	_L_hasWonPrize_B_/
	VP	<hasWonPrize>	73763
	SS	<hasWonPrize><rdf__type>	73763	1.0
	SS	<hasWonPrize><rdf__type>	73763	1.0
	SS	<hasWonPrize><rdf__type>	73763	1.0
	SS	<hasWonPrize><skos__prefLabel>	73763	1.0
------
