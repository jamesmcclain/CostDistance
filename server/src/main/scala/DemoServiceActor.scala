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

  def compute(
    sparkContext: SparkContext,
    reader: FilteringLayerReader[LayerId], writer: LayerWriter[LayerId], as: AttributeStore,
    terrainName: String, output1: String, output2: String, zoom: Int,
    x: Double, y: Double, altitude: Double
  ): Unit = {
    implicit val sc = sparkContext
    val terrainId = LayerId(terrainName, zoom)
    val terrain = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](terrainId)
    val layoutScheme = ZoomedLayoutScheme(WebMercator, 256)

    val z = 0.0
    val angle = 0.0
    val fov = -1.0

    val touched = mutable.Set.empty[SpatialKey]

    {
      val point: Array[Double] = Array(x, y, z, angle, fov, altitude)
      val before = System.currentTimeMillis
      val src = IterativeViewshed(
        terrain, List(point),
        maxDistance = 100000,
        curvature = true,
        operator = R2Viewshed.Or(),
        touched = touched)
      Pyramid.upLevels(src, layoutScheme, zoom, 1)({ (rdd, zoom) =>
        writer.write(LayerId(output1, zoom), rdd, ZCurveKeyIndexMethod) })
      val after = System.currentTimeMillis
      val millis = after - before
      as.write(LayerId(output1, 0), "millis", millis)
    }


  }
}

class DemoServiceActor(sparkContext: SparkContext, dataModel: DataModel)
    extends Actor with HttpService {
  override def actorRefFactory = context
  override def receive = runRoute(serviceRoute)
  implicit val executionContext = actorRefFactory.dispatcher

  val attributeStore = dataModel.attributeStore
  val tileReader = dataModel.tileReader

  def serviceRoute =
    pathPrefix("tms")(tms) ~
  pathPrefix("poll")(poll) ~
  pathPrefix("compute")(compute)

  // http://localhost:8777/compute?input=<input>&output1=<output1>&output2=<output2>&zoom=<zoom>&x=<x>&y=<y>&altitude=<altitude>
  def compute = {
    get({
      parameters('terrain, 'output1, 'output2, 'zoom, 'x, 'y, 'altitude)({ (terrain, output1, output2, zoom, x, y, altitude) => {
        val reader = dataModel.reader
        val writer = dataModel.writer
        val tr = new Thread(new Runnable() {
          override def run(): Unit =
            Compute.compute(
              sparkContext,
              reader, writer, attributeStore,
              terrain, output1, output2, zoom.toInt,
              x.toDouble, y.toDouble, altitude.toDouble
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

        complete(JsObject("millis" -> JsNumber(millis)))
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
          val histogram: StreamingHistogram = try {
            attributeStore
              .read[Histogram[Double]](LayerId(pyramidName, 0), "histogram")
              .asInstanceOf[StreamingHistogram]
          }
          catch {
            case e: Exception =>
              val sh = StreamingHistogram()
              sh.countItem(item = 1.0, count = 1); sh
          }
          val breaks = histogram.quantileBreaks(1<<8)
          val ramp = ColorRampMap.getOrElse(colorRamp, ColorRamps.BlueToRed).toColorMap(breaks)
          val bytes: Array[Byte] = tile.renderPng(ramp).bytes

          respondWithMediaType(MediaTypes.`image/png`)({ complete(HttpData(bytes)) })
        })
      })
    })
  }

}
