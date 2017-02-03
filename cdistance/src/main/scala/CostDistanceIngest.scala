package com.example.cdistance

import geotrellis.geotools._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.shapefile._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.vector._

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.gce.geotiff._
import org.opengis.parameter.GeneralParameterValue


object Demo {

  val logger = Logger.getLogger(Demo.getClass)

  /**
    * Dump a layer to disk.
    */
  def dump(rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]], stem: String) = {
    val mt = rdd.metadata.mapTransform

    rdd.collect.foreach({ case (k, v) =>
      val extent = mt(k)
      val pr = ProjectedRaster(Raster(v, extent), LatLng)
      val gc = pr.toGridCoverage2D
      val writer = new GeoTiffWriter(new java.io.File(s"/tmp/tif/${stem}-${System.currentTimeMillis}.tif"))
      writer.write(gc, Array.empty[GeneralParameterValue])
    })
  }

  def main(args: Array[String]) : Unit = {

    /* Spark context */
    val sparkConf = new SparkConf()
      .setAppName("Cost-Distance")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.GeowaveKryoRegistrator")
    val sparkContext = new SparkContext(sparkConf)
    implicit val sc = sparkContext

    val points: List[Point] =
      ShapeFileReader
        .readSimpleFeatures("/tmp/cost-distance/points/points.shp")
        .map({ sf => sf.toGeometry[Point] })
  }

}
