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

import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.io._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._

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
      parameters('terrain, 'output, 'zoom, 'x, 'y, 'altitude, 'maxDistance ?)(
        { (terrain, output, zoom, x, y, altitude, maxDistance) => {
          val reader = dataModel.reader
          val writer = dataModel.writer
          val tr = new Thread(new Runnable() {
            override def run(): Unit =
              Compute.observer(
                sparkContext,
                reader, writer, attributeStore,
                terrain, output, zoom.toInt,
                x.toDouble, y.toDouble, altitude.toDouble,
                maxDistance match {
                  case Some(d) => d.toDouble
                  case None => 144000.0
                }
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
      parameters('terrain, 'observer, 'output, 'zoom, 'maxDistance ?, 'magic ?)(
        { (terrain, observer, output, zoom, maxDistance, magic) => {
          val reader = dataModel.reader
          val writer = dataModel.writer
          val tr = new Thread(new Runnable() {
            override def run(): Unit =
              Compute.craft(
                sparkContext,
                reader, writer, attributeStore,
                terrain, observer, output, zoom.toInt,
                maxDistance = (maxDistance match {
                  case Some(d) => d.toDouble
                  case None => 144000.0
                }),
                magic = (magic match {
                  case Some(i) => i.toInt
                  case None => 5
                })
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
