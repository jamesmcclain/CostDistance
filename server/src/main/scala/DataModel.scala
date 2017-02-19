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

import com.typesafe.config.Config
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.vector._

import org.apache.spark.SparkContext


class DataModel(config: Config)(implicit val sc: SparkContext) {

  val (collectionReader, tileReader, attributeStore) = {
    val path = config.getString("hadoop.path")
    val attributeStore = HadoopAttributeStore(path)
    (
      HadoopCollectionLayerReader(attributeStore),
      HadoopValueReader(attributeStore),
      attributeStore
    )
  }

  // A map from layer name to that layer's maximum zoom level
  val layerNamesToMaxZooms: Map[String, Int] =
    attributeStore.layerIds
      .groupBy(_.name)
      .map { case (name, layerIds) => (name, layerIds.map(_.zoom).max) }
      .toMap

}
