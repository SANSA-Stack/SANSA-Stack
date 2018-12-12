package net.sansa_stack.ml.spark.clustering.utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import net.sansa_stack.ml.spark.clustering.datatypes.DbPOI

case class Grid(val poiArrBuf: ArrayBuffer[DbPOI], val eps: Double) {

    val startX = poiArrBuf.head.lon
    val startY = poiArrBuf.head.lat
    val gridCell = HashMap[(Int, Int), ArrayBuffer[DbPOI]]()

    init()

    private def init(): Unit = {
        var i = 0
        var j = 0
        for(dbpoi <- poiArrBuf) {
            i = math.floor( (dbpoi.lon - startX) / eps).toInt
            j = math.floor( (dbpoi.lat - startY) / eps).toInt

            gridCell.get((i, j)) match {
                case Some(cellArrBuff) => cellArrBuff.append(dbpoi)
                case None => gridCell += ( ((i, j), ArrayBuffer(dbpoi)) )
            }
        }
    }


    def getNeighbours(dbpoi: DbPOI): ArrayBuffer[DbPOI] = {

        val neighbourArrBuff = ArrayBuffer[DbPOI]()

        val celli = math.floor( (dbpoi.lon - startX) / eps).toInt
        val cellj = math.floor( (dbpoi.lat - startY) / eps).toInt
        for{
            i <- (celli - 1) to (celli + 1)
            j <- (cellj - 1) to (cellj + 1)
        }{
            gridCell.get((i, j)) match {
                case Some(cellArrBuff) => neighbourArrBuff ++= cellArrBuff
                case None => ()
            }
        }

        neighbourArrBuff.filter{
            p => (math.abs(p.lon - dbpoi.lon) <= eps) && (math.abs(p.lat - dbpoi.lat) <= eps) && p.poiId != dbpoi.poiId
        }

    }

}


