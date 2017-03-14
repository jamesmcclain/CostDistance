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
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.tiling.{ZoomedLayoutScheme, LayoutDefinition}
import geotrellis.vector.io.json.Implicits._
import geotrellis.vector._
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
    pathPrefix("poll")(poll)

  def poll = {
    get({
      pathPrefix(Segment)({ (pyramidName) =>
        val done: Boolean = try {
          attributeStore.read[Boolean](LayerId(pyramidName, 0), "done")
        }
        catch {
          case  e: Exception => false
        }

        complete(JsObject("done" -> JsBoolean(done)))
      })
    })
  }

  /** http://localhost:8777/tms/color/{name}/{z}/{x}/{y}?colorRamp=yellow-to-red-heatmap */
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

  /** http://localhost:8777/tms/bw/{name}/{z}/{x}/{y} */
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
