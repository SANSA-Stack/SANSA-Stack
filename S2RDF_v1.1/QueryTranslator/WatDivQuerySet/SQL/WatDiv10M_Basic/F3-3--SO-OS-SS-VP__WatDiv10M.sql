SELECT tab0.v1 AS v1 , tab2.v0 AS v0 , tab5.v5 AS v5 , tab4.v6 AS v6 , tab3.v4 AS v4 , tab1.v2 AS v2 
 FROM    (SELECT sub AS v0 
	 FROM wsdbm__hasGenre$$3$$
	 
	 WHERE obj = 'wsdbm:SubGenre31'
	) tab2
 JOIN    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__contentRating$$1$$
	
	) tab0
 ON(tab2.v0=tab0.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__contentSize$$2$$
	
	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT obj AS v0 , sub AS v5 
	 FROM wsdbm__purchaseFor$$6$$
	
	) tab5
 ON(tab1.v0=tab5.v0)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM wsdbm__makesPurchase$$4$$
	
	) tab3
 ON(tab5.v5=tab3.v5)
 JOIN    (SELECT sub AS v5 , obj AS v6 
	 FROM wsdbm__purchaseDate$$5$$
	
	) tab4
 ON(tab3.v5=tab4.v5)


++++++Tables Statistic
wsdbm__purchaseDate$$5$$	0	VP	wsdbm__purchaseDate/
	VP	<wsdbm__purchaseDate>	44721
	SO	<wsdbm__purchaseDate><wsdbm__makesPurchase>	44721	1.0
	SS	<wsdbm__purchaseDate><wsdbm__purchaseFor>	44721	1.0
------
wsdbm__purchaseFor$$6$$	2	OS	wsdbm__purchaseFor/sorg__contentSize
	VP	<wsdbm__purchaseFor>	150000
	OS	<wsdbm__purchaseFor><sorg__contentRating>	32864	0.22
	OS	<wsdbm__purchaseFor><sorg__contentSize>	11689	0.08
	OS	<wsdbm__purchaseFor><wsdbm__hasGenre>	150000	1.0
	SO	<wsdbm__purchaseFor><wsdbm__makesPurchase>	149998	1.0
	SS	<wsdbm__purchaseFor><wsdbm__purchaseDate>	44721	0.3
------
wsdbm__hasGenre$$3$$	2	SS	wsdbm__hasGenre/sorg__contentSize
	VP	<wsdbm__hasGenre>	58787
	SS	<wsdbm__hasGenre><sorg__contentRating>	17763	0.3
	SS	<wsdbm__hasGenre><sorg__contentSize>	5757	0.1
	SO	<wsdbm__hasGenre><wsdbm__purchaseFor>	41522	0.71
------
sorg__contentSize$$2$$	1	SS	sorg__contentSize/sorg__contentRating
	VP	<sorg__contentSize>	2438
	SS	<sorg__contentSize><sorg__contentRating>	729	0.3
	SS	<sorg__contentSize><wsdbm__hasGenre>	2438	1.0
	SO	<sorg__contentSize><wsdbm__purchaseFor>	1712	0.7
------
sorg__contentRating$$1$$	1	SS	sorg__contentRating/sorg__contentSize
	VP	<sorg__contentRating>	7530
	SS	<sorg__contentRating><sorg__contentSize>	729	0.1
	SS	<sorg__contentRating><wsdbm__hasGenre>	7530	1.0
	SO	<sorg__contentRating><wsdbm__purchaseFor>	5394	0.72
------
wsdbm__makesPurchase$$4$$	1	OS	wsdbm__makesPurchase/wsdbm__purchaseDate
	VP	<wsdbm__makesPurchase>	149998
	OS	<wsdbm__makesPurchase><wsdbm__purchaseDate>	44721	0.3
	OS	<wsdbm__makesPurchase><wsdbm__purchaseFor>	149998	1.0
------
