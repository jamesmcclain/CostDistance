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

import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import com.typesafe.config.ConfigFactory
import geotrellis.spark.LayerId
import spray.can.Http

import org.apache.spark.{SparkConf, SparkContext}


object Main {
  val sparkConf = (new SparkConf()).setAppName("TMS Server")
  val sparkContext = new SparkContext(sparkConf)
  implicit val sc = sparkContext

  val config = ConfigFactory.load()
  val staticPath = config.getString("geotrellis.server.static-path")
  val port = config.getInt("geotrellis.port")
  val host = config.getString("geotrellis.hostname")
  val pyramidName = config.getString("geotrellis.pyramid")
  val histogramId = LayerId(
    config.getString("geotrellis.hname"),
    config.getInt("geotrellis.hzoom")
  )

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("tms-server")

    val dataModel = new DataModel(config)

    // create and start our service actor
    val service = {
      val actorProps = Props(classOf[WeightedServiceActor], pyramidName, histogramId, staticPath, dataModel)
      system.actorOf(actorProps, "tms-server")
    }

    // start a new HTTP server on the given port with our service actor as the handler
    IO(Http) ! Http.Bind(service, host, port)
  }
}
