SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__caption$$1$$
	
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM sorg__contentRating$$3$$
	
	) tab2
 ON(tab0.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__text$$2$$
	) tab1
 ON(tab2.v0=tab1.v0)
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM rev__hasReview$$4$$
	
	) tab3
 ON(tab1.v0=tab3.v0)
 JOIN    (SELECT obj AS v5 , sub AS v4 
	 FROM rev__title$$5$$
	) tab4
 ON(tab3.v4=tab4.v4)
 JOIN    (SELECT obj AS v6 , sub AS v4 
	 FROM rev__reviewer$$6$$
	
	) tab5
 ON(tab4.v4=tab5.v4)
 JOIN    (SELECT sub AS v7 , obj AS v6 
	 FROM sorg__actor$$7$$
	) tab6
 ON(tab5.v6=tab6.v6)
 JOIN    (SELECT sub AS v7 , obj AS v8 
	 FROM sorg__language$$8$$
	
	) tab7
 ON(tab6.v7=tab7.v7)


++++++Tables Statistic
sorg__actor$$7$$	1	SS	sorg__actor/sorg__language
	VP	<sorg__actor>	164597
	SS	<sorg__actor><sorg__language>	43924	0.27
------
rev__reviewer$$6$$	2	SS	rev__reviewer/rev__title
	VP	<rev__reviewer>	1500000
	SO	<rev__reviewer><rev__hasReview>	1476843	0.98
	SS	<rev__reviewer><rev__title>	450056	0.3
------
sorg__contentRating$$3$$	1	SS	sorg__contentRating/sorg__caption
	VP	<sorg__contentRating>	75412
	SS	<sorg__contentRating><sorg__caption>	7422	0.1
	SS	<sorg__contentRating><sorg__text>	22700	0.3
	SS	<sorg__contentRating><rev__hasReview>	15115	0.2
------
sorg__language$$8$$	1	SS	sorg__language/sorg__actor
	VP	<sorg__language>	63899
	SS	<sorg__language><sorg__actor>	7966	0.12
------
rev__hasReview$$4$$	1	SS	rev__hasReview/sorg__caption
	VP	<rev__hasReview>	1476843
	SS	<rev__hasReview><sorg__caption>	145593	0.1
	SS	<rev__hasReview><sorg__text>	448693	0.3
	SS	<rev__hasReview><sorg__contentRating>	446476	0.3
	OS	<rev__hasReview><rev__title>	443162	0.3
	OS	<rev__hasReview><rev__reviewer>	1476843	1.0
------
sorg__caption$$1$$	3	SS	sorg__caption/rev__hasReview
	VP	<sorg__caption>	24836
	SS	<sorg__caption><sorg__text>	7427	0.3
	SS	<sorg__caption><sorg__contentRating>	7422	0.3
	SS	<sorg__caption><rev__hasReview>	4949	0.2
------
sorg__text$$2$$	1	SS	sorg__text/sorg__caption
	VP	<sorg__text>	75519
	SS	<sorg__text><sorg__caption>	7427	0.1
	SS	<sorg__text><sorg__contentRating>	22700	0.3
	SS	<sorg__text><rev__hasReview>	15087	0.2
------
rev__title$$5$$	1	SO	rev__title/rev__hasReview
	VP	<rev__title>	450056
	SO	<rev__title><rev__hasReview>	443162	0.98
	SS	<rev__title><rev__reviewer>	450056	1.0
------
