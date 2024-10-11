package net.sansa_stack.datalake.spark

import java.util

import com.google.common.collect.ArrayListMultimap
import com.typesafe.scalalogging.Logger
import net.sansa_stack.datalake.spark.utils.Helpers._
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.syntax.{ElementFilter, ElementVisitorBase, ElementWalker}

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class QueryAnalyser(query: String) {

    val logger = Logger("SANSA-DataLake")

    def getPrefixes : Map[String, String] = {
        val q = QueryFactory.create(query)
        val prolog = q.getPrologue.getPrefixMapping.getNsPrefixMap

        val prefix: Map[String, String] = invertMap(prolog)

        logger.info("\n- Prefixes: " + prefix)

        prefix
    }

    def getProject : (util.List[String], Boolean) = {
        val q = QueryFactory.create(query)
        val project = q.getResultVars

        logger.info(s"\n- Projected vars: $project")

        (project, q.isDistinct)
    }

    def getFilters : ArrayListMultimap[String, (String, String)] = {
        val q = QueryFactory.create(query)
        val filters : ArrayListMultimap[String, (String, String)] = ArrayListMultimap.create[String, (String, String)]()

        ElementWalker.walk(q.getQueryPattern, new ElementVisitorBase() { // ...when it's a block of triples...
            override def visit(ef: ElementFilter): Unit = { // ...go through all the triples...
                val bits = ef.getExpr.toString.replace("(", "").replace(")", "").split(" ", 3) // 3 not to split when the right operand is a string with possible white spaces
                val operation = bits(1)
                val leftOperand = bits(0)
                val rightOperand = bits(2)

                logger.info(s"Filter: $operation,($leftOperand,$rightOperand)")
                filters.put(operation, (leftOperand, rightOperand))
            }
        })

        filters
    }

    def getOrderBy: mutable.Set[(String, String)] = {
        val q = QueryFactory.create(query)
        var orderBys : mutable.Set[(String, String)] = mutable.Set()

        if (q.hasOrderBy) {
            val orderBy = q.getOrderBy.iterator()

            while(orderBy.hasNext) {
                val it = orderBy.next()

                orderBys += ((it.direction.toString, it.expression.toString))
            }
        } else {
            orderBys = null
        }

        orderBys
    }

    def getGroupBy(variablePredicateStar: Map[String, (String, String)], prefixes: Map[String, String]): (ListBuffer[String], mutable.Set[(String, String)]) = {
        val q = QueryFactory.create(query)
        val groupByCols : ListBuffer[String] = ListBuffer()
        var aggregationFunctions : mutable.Set[(String, String)] = mutable.Set()

        if (q.hasGroupBy) {
            val groupByVars = q.getGroupBy.getVars.asScala.toList
            for (gbv <- groupByVars) {
                val str = variablePredicateStar(gbv.toString())._1
                val vr = variablePredicateStar(gbv.toString())._2
                val ns_p = get_NS_predicate(vr)
                val column = omitQuestionMark(str) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)

                groupByCols.asJava.add(column)
            }

            val agg = q.getAggregators.asScala
            logger.info("agg: " + agg)
            for(ag <- agg) { // toPrefixString returns (aggregate_function aggregate_var) eg. (sum ?price)
                val bits = ag.getAggregator.toPrefixString.split(" ")

                val aggCol = "?" + bits(1).dropRight(1).substring(1) // ? added eg ?price in variablePredicateStar
                val str = variablePredicateStar(aggCol)._1
                val vr = variablePredicateStar(aggCol)._2
                val ns_p = get_NS_predicate(vr)
                val column = omitQuestionMark(str) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)

                aggregationFunctions += ((column, bits(0).substring(1))) // o_price_cbo -> sum
            }

            (groupByCols, aggregationFunctions)

        } else {
            null
        }

    }

    def getStars : (mutable.HashMap[String, mutable.Set[(String, String)]] with mutable.MultiMap[String, (String, String)], mutable.HashMap[(String, String), String]) = {

        val q = QueryFactory.create(query)
        val originalBGP = q.getQueryPattern.toString

        val bgp = originalBGP.replaceAll("\n", "").replaceAll("\\s+", " ").replace("{", " ").replace("}", " ") // See example below + replace breaklines + remove extra white spaces
        val triples = bgp.split("\\.(?![^\\<\\[]*[\\]\\>])")

        logger.info("\n- The BGP of the input query:  " + originalBGP)
        logger.info("\n- Number of triple-stars detected: " + triples.length)

        val stars = new mutable.HashMap[String, mutable.Set[(String, String)]] with mutable.MultiMap[String, (String, String)]
        // Multi-map to add/append elements to the value

        // Save [star]_[predicate]
        val star_pred_var : mutable.HashMap[(String, String), String] = mutable.HashMap()

        for (i <- triples.indices) { // i <- 0 until triples.length
            val triple = triples(i).trim

            logger.info(s"Triple: $triple")

            if (!triple.contains(';')) { // only one predicate attached to the subject
                val tripleBits = triple.split(" ")
                stars.addBinding(tripleBits(0), (tripleBits(1), tripleBits(2)))
                // addBinding` because standard methods like `+` will overwrite the complete key-value pair instead of adding the value to the existing key

                star_pred_var.put((tripleBits(0), tripleBits(1)), tripleBits(2))
            } else {
                val triples = triple.split(";")
                val firsTriple = triples(0)
                val firsTripleBits = firsTriple.split(" ")
                val sbj = firsTripleBits(0) // get the first triple which has s p o - rest will be only p o ;
                stars.addBinding(sbj, (firsTripleBits(1), firsTripleBits(2))) // add that first triple
                star_pred_var.put((sbj, firsTripleBits(1)), firsTripleBits(2))

                for (i <- 1 until triples.length) {
                    val t = triples(i).trim.split(" ")
                    stars.addBinding(sbj, (t(0), t(1)))

                    star_pred_var.put((sbj, t(0)), t(1))
                }
            }
        }

        (stars, star_pred_var)
    }

    def getTransformations (trans: String): (Map[String, (String, Array[String])], Map[String, Array[String]]) = {
        // Transformations
        val transformations = trans.trim().substring(1).split("&&") // E.g. [?k?a.l.+60, ?a?l.r.toInt]
        var transmap_left : Map[String, (String, Array[String])] = Map.empty
        var transmap_right : Map[String, Array[String]] = Map.empty
        for (t <- transformations) { // E.g. ?a?l.r.toInt.scl[61]
            val tbits = t.trim.split("\\.", 2) // E.g.[?a?l, r.toInt.scl(_+61)]
        val vars = tbits(0).substring(1).split("\\?") // [a, l]
        val operation = tbits(1) // E.g. r.toInt.scl(_+60)
        val temp = operation.split("\\.", 2) // E.g. [r, toInt.scl(_+61)]
        val lORr = temp(0) // E.g. r
        val functions = temp(1).split("\\.") // E.g. [toInt, scl(_+61)]
            if (lORr == "l") {
                transmap_left += (vars(0) -> (vars(1), functions))
            } else {
                transmap_right += (vars(1) -> functions)
            }

        }
        (transmap_left, transmap_right)
    }

    def hasLimit: Boolean = QueryFactory.create(query).hasLimit

    def getLimit: Int = QueryFactory.create(query).getLimit.toInt

}
