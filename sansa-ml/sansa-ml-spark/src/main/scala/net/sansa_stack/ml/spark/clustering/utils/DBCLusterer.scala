package net.sansa_stack.ml.spark.clustering.utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import net.sansa_stack.ml.spark.clustering.datatypes.DbPOI
import net.sansa_stack.ml.spark.clustering.datatypes.DbStatusEnum._

case class DBCLusterer(val eps: Double, val minPts: Int) {

    def clusterPois(poiArrBuff: ArrayBuffer[DbPOI]): ArrayBuffer[ArrayBuffer[DbPOI]] = {

        val clusterArrBuff = ArrayBuffer[ArrayBuffer[DbPOI]]()
        val grid = Grid(poiArrBuff, eps)

        for{
            dbpoi <- poiArrBuff

            if(dbpoi.dbstatus == UNDEFINED)
        }{

            val neighbourArrBuff = grid.getNeighbours(dbpoi)

            if(neighbourArrBuff.size < minPts)
            {
                dbpoi.dbstatus = NOISE
            }
            else
            {
                clusterArrBuff.append(findCluster(dbpoi, neighbourArrBuff, grid))
            }
        }

        clusterArrBuff
    }


    def findCluster(dbpoi: DbPOI, neighbourArrBuff: ArrayBuffer[DbPOI], grid: Grid): ArrayBuffer[DbPOI] = {

        dbpoi.dbstatus = PARTOFCLUSTER
        dbpoi.isDense = true

        val cluster = ArrayBuffer[DbPOI]()
        cluster.append(dbpoi)

        val neighbourQueue = mutable.Queue[DbPOI]() ++ neighbourArrBuff

        while(neighbourQueue.nonEmpty) {
            val poi = neighbourQueue.dequeue()
            poi.dbstatus match {
                case UNDEFINED =>
                    poi.dbstatus = PARTOFCLUSTER
                    val poi_i_neighbours = grid.getNeighbours(poi)
                    if(poi_i_neighbours.size >= minPts)
                    {
                        poi.isDense = true
                        neighbourQueue ++= poi_i_neighbours
                    }
                    else
                    {
                        poi.isDense = false
                    }
                    cluster.append(poi)
                case NOISE =>
                    poi.dbstatus = PARTOFCLUSTER
                    poi.isDense = false
                    cluster.append(poi)
                case _ => ()
            }
        }

        cluster
    }

}



