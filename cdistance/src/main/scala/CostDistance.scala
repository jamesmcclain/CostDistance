package com.example.cdistance

import geotrellis.geotools._
import geotrellis.proj4.{ LatLng, WebMercator }
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.costdistance._
import geotrellis.raster.io._
// import geotrellis.raster.viewshed.R2Viewshed._
import geotrellis.shapefile._
import geotrellis.spark._
import geotrellis.spark.costdistance._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.ZoomedLayoutScheme
// import geotrellis.spark.viewshed._
import geotrellis.vector._
import geotrellis.vector.io._

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.gce.geotiff._
import org.opengis.parameter.GeneralParameterValue

import com.vividsolutions.jts.{ geom => jts }


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
      val writer = new GeoTiffWriter(new java.io.File(s"/tmp/tif/${stem}-${k.col}-${k.row}.tif"))
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
      .set("spark.kryo.unsafe", "true")
      .set("spark.rdd.compress", "true")
    val sparkContext = new SparkContext(sparkConf)
    implicit val sc = sparkContext

    // // VIEWSHED COMMAND
    // if (operation == "viewshed") {
    //   val zoom = args(4).toInt
    //   val readId = LayerId(args(2), zoom)
    //   val writeId = LayerId(args(3), zoom)
    //   val maxDistance = args(5).toDouble
    //   val op = args(6) match {
    //     case "AND" => And
    //     case "DEBUG" => Debug
    //     case "OR" => Or
    //   }

    //   val points = args.drop(7)
    //     .grouped(6)
    //     .toList
    //     .map({ case _ar: Array[String] if (_ar.length == 6) =>
    //       val ar = _ar.map(_.toDouble)
    //       if (ar(5) == 0) ar(5) = Double.NegativeInfinity
    //       ar })
    //     .map({ case Array(a, b, c, d, e, f) => IterativeViewshed.Point6D(a, b, c, d, e, f) })

    //   logger.debug(s"Viewshed: catalog=$catalog input=$readId output=$writeId maxDistance=$maxDistance op=$op points=${points}")

    //   // Read elevation layer
    //   val elevation =
    //     HadoopLayerReader(catalog)
    //       .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)

    //   // Compute viewshed layer
    //   val before = System.currentTimeMillis
    //   val viewshed = IterativeViewshed(
    //     elevation, points,
    //     maxDistance = maxDistance,
    //     curvature = true,
    //     operator = op
    //   )
    //   val after = System.currentTimeMillis

    //   logger.info(s"${after - before} milliseconds")
    //   logger.info(s"Writing to $catalog $writeId")
    //   HadoopLayerWriter(catalog).write(writeId, viewshed, GeowaveKeyIndexMethod)
    // }
    // PYRAMID COMMAND
    /*else*/ if (operation == "pyramid") {
      val inputZoom = args(3).toInt
      val outputLayerName = args(4)
      val size = args(5).toInt
      val readId = LayerId(args(2), inputZoom)
      val _src =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)
      val src =
        if (_src.partitions.length >= (1<<7)) _src
        else ContextRDD(_src.repartition((1<<7)), _src.metadata)
      val layoutScheme = ZoomedLayoutScheme(WebMercator, size)

      logger.debug(s"Pyramid: catalog=$catalog input=$readId ${size}×${size} tiles")
      Pyramid.upLevels(src, layoutScheme, inputZoom, 1)({ (rdd, outputZoom) =>
        val writeId = LayerId(outputLayerName, outputZoom)

        logger.info(s"Writing to $catalog $writeId")
        HadoopLayerWriter(catalog).write(writeId, rdd, GeowaveKeyIndexMethod)
      })
    }
    // MASK
    else if (operation == "mask") {
      val zoom = args(4).toInt
      val readId = LayerId(args(2), zoom)
      val writeId = LayerId(args(3), zoom)
      val geojsonUri = args(5)
      val polygon =
        scala.io.Source.fromFile(geojsonUri, "UTF-8")
          .getLines
          .mkString
          .extractGeometries[MultiPolygon]
          .head
          .reproject(LatLng, WebMercator)

      logger.debug(s"Mask: catalog=$catalog input=$readId output=$writeId polygon=$geojsonUri")

      val src =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)
      val masked = src.mask(polygon)

      HadoopLayerWriter(catalog).write(writeId, masked, GeowaveKeyIndexMethod)
    }
    // COPY COMMAND
    else if (operation == "copy") {
      val zoom = args(4).toInt
      val readId = LayerId(args(2), zoom)
      val writeId = LayerId(args(3), zoom)

      logger.debug(s"Copy: catalog=$catalog input=$readId output=$writeId")

      val src =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](readId)
      HadoopLayerWriter(catalog).write(writeId, src, GeowaveKeyIndexMethod)
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
      HadoopLayerWriter(catalog).write(writeId, slope, GeowaveKeyIndexMethod)
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
      HadoopLayerWriter(catalog).write(writeId, cost, GeowaveKeyIndexMethod)
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
      val id1 = LayerId(args(2), args(3).toInt)
      val id2 = LayerId(args(4), args(5).toInt)
      val attributeStore = HadoopAttributeStore(catalog)
      val layer =
        HadoopLayerReader(catalog)
          .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id1)

      logger.debug(s"Histogram: catalog=$catalog input=$id1 output=$id2")
      attributeStore.write(id2, "histogram", layer.histogram())
    }
    // ANY OTHER COMMAND
    else logger.debug(s"Unknown Operaton: $operation")

    sparkContext.stop
  }
}
