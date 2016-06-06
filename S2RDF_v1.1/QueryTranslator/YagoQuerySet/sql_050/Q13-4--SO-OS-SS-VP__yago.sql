SELECT tab6.country1 AS country1 , tab2.area AS area , tab4.geo AS geo , tab11.lon2 AS lon2 , tab0.lon1 AS lon1 , tab7.country2 AS country2 , tab9.name AS name , tab8.property AS property , tab3.wiki AS wiki , tab5.lang AS lang , tab7.capital AS capital , tab12.lat2 AS lat2 , tab10.url AS url , tab1.lat1 AS lat1 
 FROM    (SELECT sub AS country2 , obj AS capital 
	 FROM _L_hasCapital_B_$$8$$
	) tab7
 JOIN    (SELECT obj AS property , sub AS capital 
	 FROM _L_owns_B_$$9$$
	) tab8
 ON(tab7.capital=tab8.capital)
 JOIN    (SELECT sub AS country1 , obj AS country2 
	 FROM _L_dealsWith_B_$$7$$
	) tab6
 ON(tab7.country2=tab6.country2)
 JOIN    (SELECT sub AS country1 , obj AS lang 
	 FROM _L_hasOfficialLanguage_B_$$6$$
	
	) tab5
 ON(tab6.country1=tab5.country1)
 JOIN    (SELECT sub AS property , obj AS url 
	 FROM _L_hasWebsite_B_$$11$$
	
	) tab10
 ON(tab8.property=tab10.property)
 JOIN    (SELECT sub AS property , obj AS lat2 
	 FROM _L_hasLatitude_B_$$13$$
	
	) tab12
 ON(tab10.property=tab12.property)
 JOIN    (SELECT obj AS lon2 , sub AS property 
	 FROM _L_hasLongitude_B_$$12$$
	
	) tab11
 ON(tab12.property=tab11.property)
 JOIN    (SELECT obj AS name , sub AS property 
	 FROM skos__prefLabel$$10$$
	
	) tab9
 ON(tab11.property=tab9.property)
 JOIN    (SELECT sub AS geo , obj AS lang 
	 FROM _L_linksTo_B_$$5$$
	) tab4
 ON(tab5.lang=tab4.lang)
 JOIN    (SELECT obj AS area , sub AS geo 
	 FROM _L_hasArea_B_$$3$$
	) tab2
 ON(tab4.geo=tab2.geo)
 JOIN    (SELECT sub AS geo , obj AS wiki 
	 FROM _L_hasWikipediaUrl_B_$$4$$
	
	) tab3
 ON(tab2.geo=tab3.geo)
 JOIN    (SELECT sub AS geo , obj AS lat1 
	 FROM _L_hasLatitude_B_$$2$$
	
	) tab1
 ON(tab3.geo=tab1.geo)
 JOIN    (SELECT sub AS geo , obj AS lon1 
	 FROM _L_hasLongitude_B_$$1$$
	
	) tab0
 ON(tab1.geo=tab0.geo)


++++++Tables Statistic
_L_dealsWith_B_$$7$$	0	VP	_L_dealsWith_B_/
	VP	<dealsWith>	947
	SS	<dealsWith><hasOfficialLanguage>	854	0.9
	OS	<dealsWith><hasCapital>	886	0.94
------
_L_hasCapital_B_$$8$$	1	SO	_L_hasCapital_B_/_L_dealsWith_B_
	VP	<hasCapital>	1937
	SO	<hasCapital><dealsWith>	118	0.06
	OS	<hasCapital><owns>	291	0.15
------
_L_hasLongitude_B_$$12$$	1	SO	_L_hasLongitude_B_/_L_owns_B_
	VP	<hasLongitude>	7554653
	SO	<hasLongitude><owns>	5435	0.0
	SS	<hasLongitude><skos__prefLabel>	989931	0.13
	SS	<hasLongitude><hasWebsite>	127667	0.02
	SS	<hasLongitude><hasLatitude>	7554274	1.0
------
_L_hasOfficialLanguage_B_$$6$$	1	SS	_L_hasOfficialLanguage_B_/_L_dealsWith_B_
	VP	<hasOfficialLanguage>	964
	SS	<hasOfficialLanguage><dealsWith>	175	0.18
------
_L_hasLatitude_B_$$13$$	1	SO	_L_hasLatitude_B_/_L_owns_B_
	VP	<hasLatitude>	7554186
	SO	<hasLatitude><owns>	5429	0.0
	SS	<hasLatitude><skos__prefLabel>	989463	0.13
	SS	<hasLatitude><hasWebsite>	127658	0.02
	SS	<hasLatitude><hasLongitude>	7554088	1.0
------
_L_owns_B_$$9$$	1	SO	_L_owns_B_/_L_hasCapital_B_
	VP	<owns>	26551
	SO	<owns><hasCapital>	332	0.01
	OS	<owns><skos__prefLabel>	26532	1.0
	OS	<owns><hasWebsite>	2605	0.1
	OS	<owns><hasLongitude>	3093	0.12
	OS	<owns><hasLatitude>	3095	0.12
------
skos__prefLabel$$10$$	1	SO	skos__prefLabel/_L_owns_B_
	VP	<skos__prefLabel>	2954875
	SO	<skos__prefLabel><owns>	24409	0.01
	SS	<skos__prefLabel><hasWebsite>	191928	0.06
	SS	<skos__prefLabel><hasLongitude>	404089	0.14
	SS	<skos__prefLabel><hasLatitude>	403906	0.14
------
_L_hasLongitude_B_$$1$$	2	SS	_L_hasLongitude_B_/_L_hasArea_B_
	VP	<hasLongitude>	7554653
	SS	<hasLongitude><hasLatitude>	7554274	1.0
	SS	<hasLongitude><hasArea>	297538	0.04
	SS	<hasLongitude><hasWikipediaUrl>	990334	0.13
	SS	<hasLongitude><linksTo>	987301	0.13
------
_L_hasLatitude_B_$$2$$	2	SS	_L_hasLatitude_B_/_L_hasArea_B_
	VP	<hasLatitude>	7554186
	SS	<hasLatitude><hasLongitude>	7554088	1.0
	SS	<hasLatitude><hasArea>	297485	0.04
	SS	<hasLatitude><hasWikipediaUrl>	989867	0.13
	SS	<hasLatitude><linksTo>	986834	0.13
------
_L_hasArea_B_$$3$$	0	VP	_L_hasArea_B_/
	VP	<hasArea>	129920
	SS	<hasArea><hasLongitude>	114389	0.88
	SS	<hasArea><hasLatitude>	114376	0.88
	SS	<hasArea><hasWikipediaUrl>	129920	1.0
	SS	<hasArea><linksTo>	129909	1.0
------
_L_linksTo_B_$$5$$	3	SS	_L_linksTo_B_/_L_hasArea_B_
	VP	<linksTo>	38048450
	SS	<linksTo><hasLongitude>	4658175	0.12
	SS	<linksTo><hasLatitude>	4656823	0.12
	SS	<linksTo><hasArea>	1849528	0.05
	SS	<linksTo><hasWikipediaUrl>	38048450	1.0
------
_L_hasWikipediaUrl_B_$$4$$	3	SS	_L_hasWikipediaUrl_B_/_L_hasArea_B_
	VP	<hasWikipediaUrl>	2886878
	SS	<hasWikipediaUrl><hasLongitude>	404694	0.14
	SS	<hasWikipediaUrl><hasLatitude>	404509	0.14
	SS	<hasWikipediaUrl><hasArea>	130089	0.05
	SS	<hasWikipediaUrl><linksTo>	2826443	0.98
------
_L_hasWebsite_B_$$11$$	1	SO	_L_hasWebsite_B_/_L_owns_B_
	VP	<hasWebsite>	226393
	SO	<hasWebsite><owns>	2769	0.01
	SS	<hasWebsite><skos__prefLabel>	226365	1.0
	SS	<hasWebsite><hasLongitude>	55500	0.25
	SS	<hasWebsite><hasLatitude>	55486	0.25
------
