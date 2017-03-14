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
import geotrellis.raster.viewshed.R2Viewshed._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
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
import scala.concurrent.Future


object Compute {
  implicit object BooleanFormat extends RootJsonFormat[Boolean] {
    def write(bit: Boolean) =
      JsObject("bit" -> JsBoolean(bit))

    def read(value: JsValue): Boolean =
      value.asJsObject.getFields("bit") match {
        case Seq(JsBoolean(bit)) =>
          bit
        case _ =>
          throw new DeserializationException("Boolean expected")
      }
  }

  def compute(
    reader: FilteringLayerReader[LayerId], writer: LayerWriter[LayerId],
    input: String, output: String, zoom: Int,
    x: Double, y: Double, altitude: Double
  ): Unit = {
    val inputLayerId = LayerId(input, zoom)
    val observerLayerId = LayerId(s"${output}-observer", 0)
    val craftLayerId = LayerId(s"${output}-craft", 0)

    println(s"$reader $writer $input $output $zoom $x $y $altitude")
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
    pathPrefix("tms" / "color")(colorTms) ~
    pathPrefix("tms" / "monocrhome")(monochromeTms) ~
    pathPrefix("poll")(poll) ~
    pathPrefix("compute")(compute)

  // http://localhost:8777/compute?input=<input>&output=<output>&zoom=<zoom>&x=<x>&y=<y>&altitude=<altitude>
  def compute = {
    get({
      parameters('input, 'output, 'zoom, 'x, 'y, 'altitude)({ (input, output, zoom, x, y, altitude) => {
        val reader = dataModel.reader
        val writer = dataModel.writer
        val tr = new Thread(new Runnable() {
          override def run(): Unit =
            Compute.compute(reader, writer, input, output, zoom.toInt, x.toDouble, y.toDouble, altitude.toDouble)
        })

        tr.start
        complete(JsObject(
          "observer" -> JsString(s"${output}-observer"),
          "craft" -> JsString(s"${output}-craft")))
      }})
    })
  }

  // http://localhost:8777/poll?name=<name>
  def poll = {
    get({
      parameters('name)({ (name) => {
        val done: Boolean = try {
          attributeStore.read[Boolean](LayerId(name, 0), "done")
        }
        catch {
          case  e: Exception => false
        }

        complete(JsObject("done" -> JsBoolean(done)))
      }})
    })
  }

  // http://localhost:8777/tms/color/{name}/{z}/{x}/{y}?colorRamp=yellow-to-red-heatmap
  def colorTms = {
    get({
      pathPrefix(Segment / IntNumber / IntNumber / IntNumber)({ (pyramidName, zoom, x, y) =>
        parameters('colorRamp ? "blue-to-red")({ (colorRamp) =>
          val key = SpatialKey(x, y)
          val tile = tileReader
            .reader[SpatialKey, Tile](LayerId(pyramidName, zoom))
            .read(key)
          val histogram = attributeStore
            .read[Histogram[Double]](LayerId(pyramidName, 0), "histogram")
            .asInstanceOf[StreamingHistogram]
          val breaks = histogram.quantileBreaks(1<<8)
          val ramp = ColorRampMap.getOrElse(colorRamp, ColorRamps.BlueToRed).toColorMap(breaks)

          respondWithMediaType(MediaTypes.`image/png`)({
            complete(tile.renderPng(ramp).bytes)
          })
        })
      })
    })
  }

  // http://localhost:8777/tms/monochrome/{name}/{z}/{x}/{y}
  def monochromeTms = {
    get({
      pathPrefix(Segment / IntNumber / IntNumber / IntNumber)({ (pyramidName, zoom, x, y) =>
        parameters('colorRamp ? "blue-to-red")({ (colorRamp) =>
          val key = SpatialKey(x, y)
          val tile = tileReader
            .reader[SpatialKey, Tile](LayerId(pyramidName, zoom))
            .read(key)

          respondWithMediaType(MediaTypes.`image/png`)({
            complete(tile.renderPng.bytes)
          })
        })
      })
    })
  }
}
