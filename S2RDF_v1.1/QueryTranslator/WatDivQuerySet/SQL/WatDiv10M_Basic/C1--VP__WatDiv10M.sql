SELECT tab0.v1 AS v1 , tab0.v0 AS v0 , tab6.v7 AS v7 , tab4.v5 AS v5 , tab5.v6 AS v6 , tab3.v4 AS v4 , tab2.v3 AS v3 , tab7.v8 AS v8 , tab1.v2 AS v2 
 FROM    (SELECT obj AS v1 , sub AS v0 
	 FROM sorg__caption$$1$$
	
	) tab0
 JOIN    (SELECT sub AS v0 , obj AS v2 
	 FROM sorg__text$$2$$
	) tab1
 ON(tab0.v0=tab1.v0)
 JOIN    (SELECT sub AS v0 , obj AS v3 
	 FROM sorg__contentRating$$3$$
	
	) tab2
 ON(tab1.v0=tab2.v0)
 JOIN    (SELECT sub AS v0 , obj AS v4 
	 FROM rev__hasReview$$4$$
	
	) tab3
 ON(tab2.v0=tab3.v0)
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
sorg__actor$$7$$	0	VP	sorg__actor/
	VP	<sorg__actor>	15991
------
rev__reviewer$$6$$	0	VP	rev__reviewer/
	VP	<rev__reviewer>	150000
------
sorg__contentRating$$3$$	0	VP	sorg__contentRating/
	VP	<sorg__contentRating>	7530
------
sorg__language$$8$$	0	VP	sorg__language/
	VP	<sorg__language>	6251
------
rev__hasReview$$4$$	0	VP	rev__hasReview/
	VP	<rev__hasReview>	149634
------
sorg__caption$$1$$	0	VP	sorg__caption/
	VP	<sorg__caption>	2501
------
sorg__text$$2$$	0	VP	sorg__text/
	VP	<sorg__text>	7476
------
rev__title$$5$$	0	VP	rev__title/
	VP	<rev__title>	44830
------
