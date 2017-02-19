package com.example.cdistance

import geotrellis.geotools._
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.costdistance._
import geotrellis.raster.io._
import geotrellis.shapefile._
import geotrellis.spark._
import geotrellis.spark.costdistance._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.ZoomedLayoutScheme
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
    val operation = args(1)

    // Establish Spark Context
    val sparkConf = (new SparkConf())
      .setAppName("Cost-Distance")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .set("spark.rdd.compress", "true")
    val sparkContext = new SparkContext(sparkConf)
    implicit val sc = sparkContext

    // PYRAMID COMMAND
    if (operation == "pyramid") {
      val inputZoom = args(3).toInt
      val outputLayerName = args(4)
      val readId = LayerId(args(2), inputZoom)
      val src =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)
      val layoutScheme = ZoomedLayoutScheme(WebMercator, 512)

      logger.debug(s"Pyramid: catalog=$catalog input=$readId")
      Pyramid.upLevels(src, layoutScheme, 12, 1)({ (rdd, outputZoom) =>
        val writeId = LayerId(outputLayerName, outputZoom)

        logger.info(s"Writing to $catalog $writeId")
        HadoopLayerWriter(catalog).write(writeId, rdd, ZCurveKeyIndexMethod)
      })
    }
    // SLOPE COMMAND
    else if (operation == "slope") {
      val zoom = args(4).toInt
      val readId = LayerId(args(2), zoom)
      val writeId = LayerId(args(3), zoom)

      logger.debug(s"Slope: catalog=$catalog input=$readId output=$writeId")

      val elevation =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)
      val slope = elevation.slope()

      logger.info(s"Writing to $catalog $writeId")
      HadoopLayerWriter(catalog).write(writeId, slope, ZCurveKeyIndexMethod)
    }
    // COST-DISTANCE COMMAND
    else if (operation == "costdistance") {
      val zoom = args(4).toInt
      val readId = LayerId(args(2), zoom)
      val writeId = LayerId(args(3), zoom)
      val shapeFile = args(5)
      val maxCost = args(6).toDouble

      logger.debug(s"Cost-Distance: catalog=$catalog input=$readId output=$writeId shapeFile=$shapeFile maxCost=$maxCost")

      // Read friction layer
      val friction =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)

      // Read starting points
      val points: List[Point] =
        ShapeFileReader
          .readSimpleFeatures(shapeFile)
          .map({ sf => sf.toGeometry[Point] })

      // Compute cost layer
      val before = System.currentTimeMillis
      val cost = friction.costdistance(points, maxCost)
      val after = System.currentTimeMillis

      logger.info(s"${after - before} milliseconds")
      logger.info(s"Writing to $catalog $writeId")
      HadoopLayerWriter(catalog).write(writeId, cost, ZCurveKeyIndexMethod)
    }
    // DUMP COMMAND
    else if (operation == "dump") {
      val layerName = args(2)
      val id = LayerId(layerName, args(3).toInt)
      val layer =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)

      logger.debug(s"Dump: catalog=$catalog input=$id output=/tmp/tif/${layerName}-*.tif")
      dump(layer, layerName)
    }
    // HISTOGRAM
    else if (operation == "histogram") {
      val layerName = args(2)
      val id = LayerId(layerName, args(3).toInt)
      val attributeStore = HadoopAttributeStore(catalog)
      val layer =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)

      logger.debug(s"Histogram: catalog=$catalog input=$id")
      attributeStore.write(id, "histogram", layer.histogram())
    }
    // ANY OTHER COMMAND
    else logger.debug(s"Unknown Operaton: $operation")

    sparkContext.stop
  }
}
