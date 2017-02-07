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

    rdd.collect.foreach({ case (k, v) =>
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
    val shapeFile = args(1)
    val save: Option[String] = if (args.length > 2) Some(args(2)); else None
    val maxCost = if (args.length > 3) args(3).toDouble; else 144000.0

    // Establish Spark Context
    val sparkConf = (new SparkConf()).setAppName("Cost-Distance")
    val sparkContext = new SparkContext(sparkConf)
    implicit val sc = sparkContext

    if (save != Some("dump-only")) {
      // Get friction tiles
      val id = LayerId("friction", 0)
      val friction =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)

      // Get starting points
      val points: List[Point] =
        ShapeFileReader
          .readSimpleFeatures(shapeFile)
          .map({ sf => sf.toGeometry[Point] })

      // Cost
      val before = System.currentTimeMillis
      val cost = ContextRDD(IterativeCostDistance(friction, points, maxCost), friction.metadata)
      val after = System.currentTimeMillis

      // Report Timing
      logger.info(s"MILLIS: ${after - before}")

      save match {
        case Some("dump") => dump(cost, "cost")
        case Some(layerName) => HadoopLayerWriter(catalog).write(LayerId(layerName, 0), cost, ZCurveKeyIndexMethod)
        case None =>
      }
    }
    else {
      val layerName = args(1)
      val cost =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](LayerId(layerName, 0))
      dump(cost, layerName)
    }
  }
}
