/*
 * Copyright (c) 2017 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.server

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.io._
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.render._
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.viewshed.R2Viewshed
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{ZoomedLayoutScheme, LayoutDefinition}
import geotrellis.spark.viewshed._
import geotrellis.vector._
import geotrellis.vector.io.json.Implicits._
import geotrellis.vector.reproject._

import akka.actor._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future


object Compute {

  val logger = Logger.getLogger(Compute.getClass)
  val layoutScheme = ZoomedLayoutScheme(WebMercator, 256)

  val z = 0.0
  val angle = 0.0
  val fov = -1.0

  /**
    *
    */
  def observer(
    sparkContext: SparkContext,
    reader: FilteringLayerReader[LayerId], writer: LayerWriter[LayerId], as: AttributeStore,
    terrainName: String, output: String, zoom: Int,
    x: Double, y: Double, altitude: Double
  ): Unit = {
    implicit val sc = sparkContext
    val terrainId = LayerId(terrainName, zoom)
    val outputId = LayerId(output, 0)
    val terrain = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](terrainId)
    val touched = mutable.Set.empty[SpatialKey]
    val point: Array[Double] = Array(x, y, z, angle, fov, altitude)
    val before = System.currentTimeMillis
    val src = IterativeViewshed(
      terrain, List(point),
      maxDistance = 100000,
      curvature = true,
      operator = R2Viewshed.Or(),
      touched = touched)
    val after1 = System.currentTimeMillis
    logger.info(s"Observer viewshed computed in ${after1 - before} ms, ${touched.size} tiles touched")
    Pyramid.upLevels(src, layoutScheme, zoom, 1)({ (rdd, zoom) =>
      logger.info(s"Level $zoom observer viewshed stored")
      writer.write(LayerId(output, zoom), rdd, ZCurveKeyIndexMethod) })
    val after2 = System.currentTimeMillis
    val millis = after2 - before
    logger.info(s"Observer viewshed+pyramid computed in $millis ms")

    as.write(outputId, "altitude", altitude)
    as.write(outputId, "touched", touched.toList)
    as.write(outputId, "millis", millis)
  }

  private def combine(
    a: mutable.ArrayBuffer[Array[Double]],
    b: mutable.ArrayBuffer[Array[Double]]) = {
    a ++ b
  }

  /**
    *
    */
  def craft(
    sparkContext: SparkContext,
    reader: FilteringLayerReader[LayerId], writer: LayerWriter[LayerId], as: AttributeStore,
    terrainName: String, observerName: String, output: String, zoom: Int
  ): Unit = {
    implicit val sc = sparkContext
    val terrainId = LayerId(terrainName, zoom)
    val terrain = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](terrainId)
    val observerId0 = LayerId(observerName, 0)
    val observerIdZ = LayerId(observerName, zoom)
    val observer = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](observerIdZ)
    val outputId = LayerId(output, 0)
    val altitude = as.read[Double](observerId0, "altitude")
    val touched = as.read[List[SpatialKey]](observerId0, "touched").toSet
    val mt = observer.metadata.mapTransform
    val points: Seq[Array[Double]] = {
      observer
        .filter({ case (k, _) => touched.contains(k) })
        .map({ case (k, v) =>
          val extent = mt(k)
          val cols = v.cols
          val rows = v.rows
          val re = RasterExtent(extent, cols, rows)
          val points = mutable.ArrayBuffer.empty[Array[Double]]

          v.foreach({ (col, row, z) =>
            if (isData(z) && (col % 3 == 0) && (row % 5 == 0) && (z > 0)) {
              val (x, y) = re.gridToMap(col, row)
              points.append(Array(x, y, altitude, 0.0, -1.0, Double.NegativeInfinity))
            }})
          points })
        .aggregate(mutable.ArrayBuffer.empty[Array[Double]])(combine, combine)
    }
    logger.info(s"${points.length} points")
    val before = System.currentTimeMillis
    val src = IterativeViewshed(
      terrain, points,
      maxDistance = 100000,
      curvature = true,
      operator = R2Viewshed.Plus()
    )
    val after1 = System.currentTimeMillis
    logger.info(s"Craft viewshed computed in ${after1 - before} ms")
    Pyramid.upLevels(src, layoutScheme, zoom, 1)({ (rdd, zoom) =>
      logger.info(s"Level $zoom craft viewshed stored")
      writer.write(LayerId(output, zoom), rdd, ZCurveKeyIndexMethod) })
    val after2 = System.currentTimeMillis
    val millis = after2 - before
    logger.info(s"Craft viewshed+pyramid computed in $millis ms")

    as.write(outputId, "points", points.length)
    as.write(outputId, "millis", millis)
  }

}

class DemoServiceActor(sparkContext: SparkContext, dataModel: DataModel)
    extends Actor with HttpService {
  override def actorRefFactory = context
  override def receive = runRoute(serviceRoute)
  implicit val executionContext = actorRefFactory.dispatcher

  val attributeStore = dataModel.attributeStore
  val tileReader = dataModel.tileReader
  val breaksMap = mutable.Map.empty[String, Array[Double]]

  def serviceRoute =
    pathPrefix("tms")(tms) ~
    pathPrefix("poll")(poll) ~
    pathPrefix("observer")(observer) ~
    pathPrefix("craft")(craft)

  // http://localhost:8777/observer?terrain=<terrain>&output=<output>&zoom=<zoom>&x=<x>&y=<y>&altitude=<altitude>
  def observer = {
    get({
      parameters('terrain, 'output, 'zoom, 'x, 'y, 'altitude)({ (terrain, output, zoom, x, y, altitude) => {
        val reader = dataModel.reader
        val writer = dataModel.writer
        val tr = new Thread(new Runnable() {
          override def run(): Unit =
            Compute.observer(
              sparkContext,
              reader, writer, attributeStore,
              terrain, output, zoom.toInt,
              x.toDouble, y.toDouble, altitude.toDouble
            )
        })

        tr.start
        complete("ok")
      }})
    })
  }

  // http://localhost:8777/craft?terrain=<terrain>&observer=<observer>&output=<output>&zoom=<zoom>
  def craft = {
    get({
      parameters('terrain, 'observer, 'output, 'zoom)({ (terrain, observer, output, zoom) => {
        val reader = dataModel.reader
        val writer = dataModel.writer
        val tr = new Thread(new Runnable() {
          override def run(): Unit =
            Compute.craft(
              sparkContext,
              reader, writer, attributeStore,
              terrain, observer, output, zoom.toInt
            )
        })

        tr.start
        complete("ok")
      }})
    })
  }

  // http://localhost:8777/poll?name=<name>
  def poll = {
    get({
      parameters('name)({ (name) => {
        val millis: Long = try {
          attributeStore.read[Int](LayerId(name, 0), "millis")
        }
        catch {
          case  e: Exception => -1
        }

        complete(if (millis > -1) "ok"; else "no")
      }})
    })
  }

  // http://localhost:8777/tms/{name}/{z}/{x}/{y}?colorRamp=yellow-to-red-heatmap
  def tms = {
    get({
      pathPrefix(Segment / IntNumber / IntNumber / IntNumber)({ (pyramidName, zoom, x, y) =>
        parameters('colorRamp ? "blue-to-red")({ (colorRamp) =>
          val key = SpatialKey(x, y)
          val tile = tileReader
            .reader[SpatialKey, Tile](LayerId(pyramidName, zoom))
            .read(key)
          val breaks: Array[Double] = breaksMap.getOrElse(pyramidName, breaksMap.synchronized {

            val histogram: Option[StreamingHistogram] = try { // fetch histogram (if possible)
              val histogram: StreamingHistogram = attributeStore
                .read[Histogram[Double]](LayerId(pyramidName, 0), "histogram")
                .asInstanceOf[StreamingHistogram]
              Some(histogram)
            } catch { case e: Exception => None }

            val breaks: Array[Double] = histogram match { // compute breaks
              case Some(histogram) => histogram.quantileBreaks(1<<8)
              case None =>
                try {
                  val points = attributeStore.read[Int](LayerId(pyramidName, 0), "points")
                  (0 to points).map(_.toDouble).toArray
                } catch { case e: Exception => Array[Double](0.0, 1.0) }
            }

            breaksMap += ((pyramidName, breaks)) // remember breaks
            breaks // return breaks
          })
          val ramp = ColorRampMap.getOrElse(colorRamp, ColorRamps.BlueToRed).toColorMap(breaks)
          val bytes: Array[Byte] = tile.renderPng(ramp).bytes

          respondWithMediaType(MediaTypes.`image/png`)({ complete(HttpData(bytes)) })
        })
      })
    })
  }

}
