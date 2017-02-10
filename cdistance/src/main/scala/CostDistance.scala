package com.example.cdistance

import geotrellis.geotools._
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.costdistance._
import geotrellis.shapefile._
import geotrellis.spark._
import geotrellis.spark.costdistance._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.vector._

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.gce.geotiff._
import org.opengis.parameter.GeneralParameterValue


object CostDistance {

  val logger = Logger.getLogger(CostDistance.getClass)

  /**
    * Dump a layer to disk.
    */
  def dump(rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]], stem: String) = {
    val mt = rdd.metadata.mapTransform

    rdd.foreach({ case (k, v) =>
      val extent = mt(k)
      val pr = ProjectedRaster(Raster(v, extent), WebMercator)
      val gc = pr.toGridCoverage2D
      val writer = new GeoTiffWriter(new java.io.File(s"/tmp/tif/${stem}-${System.currentTimeMillis}.tif"))
      writer.write(gc, Array.empty[GeneralParameterValue])
    })
  }

  /**
    * Main
    */
  def main(args: Array[String]) : Unit = {
    val catalog = args(0)
    val save = args(1)

    // Establish Spark Context
    val sparkConf = (new SparkConf()).setAppName("Cost-Distance")
    val sparkContext = new SparkContext(sparkConf)
    implicit val sc = sparkContext

    if (save != "dump") {
      val frictionLayerName = args(2)
      val zoom = args(3).toInt
      val shapeFile = args(4)
      val maxCost = args(5).toDouble
      val rid = LayerId(frictionLayerName, zoom)
      logger.debug(s"catalog=$catalog frictionLayer=$rid shapeFile=$shapeFile maxCost=$maxCost")

      // Get friction tiles
      val friction =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](rid)

      // Get starting points
      val points: List[Point] =
        ShapeFileReader
          .readSimpleFeatures(shapeFile)
          .map({ sf => sf.toGeometry[Point] })

      // Cost
      val before = System.currentTimeMillis
      val cost = ContextRDD(IterativeCostDistance(friction, points, maxCost), friction.metadata)
      val after = System.currentTimeMillis

      // Report timing
      logger.info(s"${after - before} milliseconds")

      // Write results
      val wid = LayerId(save, zoom)
      logger.info(s"Writing to $catalog $wid")
      HadoopLayerWriter(catalog).write(wid, cost, ZCurveKeyIndexMethod)
    }
    else if (save == "dump") {
      val costLayerName = args(2)
      val zoom = args(3).toInt
      val id = LayerId(costLayerName, zoom)
      val cost =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)

      dump(cost, costLayerName)
    }
  }
}
