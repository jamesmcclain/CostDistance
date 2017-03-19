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

import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.viewshed.IterativeViewshed.Point6D
import geotrellis.vector._

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.mutable


object Compute {

  private val logger = Logger.getLogger(Compute.getClass)
  private val layoutScheme = ZoomedLayoutScheme(WebMercator, 256)

  /**
    * Compute the viewshed for an observer at ground level at the
    * given place w.r.t. objects at the given altitude.
    */
  def observer(
    sparkContext: SparkContext,
    reader: FilteringLayerReader[LayerId],
    writer: LayerWriter[LayerId],
    attributeStore: AttributeStore,
    terrainName: String,
    outputName: String,
    zoom: Int,
    x: Double, y: Double, altitude: Double,
    maxDistance: Double
  ): Unit = {
    val zee = 0.0
    val angle = 0.0
    val fov = -1.0

    implicit val sc = sparkContext
    val terrainId = LayerId(terrainName, zoom)
    val _terrain = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](terrainId)
    val terrain =
      if (_terrain.partitions.length >= (1<<7)) _terrain
      else ContextRDD(_terrain.repartition((1<<7)), _terrain.metadata)
    val outputId = LayerId(outputName, 0)
    val touched = mutable.Set.empty[SpatialKey]
    val point: Point6D = Array(x, y, zee, angle, fov, altitude)

    val before = System.currentTimeMillis
    val src = terrain.viewshed(
      points = List(point),
      maxDistance = maxDistance,
      touchedKeys = touched
    )
    val after1 = System.currentTimeMillis
    logger.info(s"Observer viewshed computed in ${after1 - before} ms, ${touched.size} tiles touched")

    Pyramid.upLevels(src, layoutScheme, zoom, 1)({ (rdd, zoom) =>
      logger.info(s"Level $zoom observer viewshed stored")
      writer.write(LayerId(outputName, zoom), rdd, ZCurveKeyIndexMethod) })
    val after2 = System.currentTimeMillis
    val millis = after2 - before
    logger.info(s"Observer viewshed+pyramid computed in $millis ms")

    attributeStore.write(outputId, "altitude", altitude)
    attributeStore.write(outputId, "touched", touched.toList)
    attributeStore.write(outputId, "millis", millis)
  }

  private def combine(
    a: mutable.ArrayBuffer[Point6D],
    b: mutable.ArrayBuffer[Point6D]) = {
    a ++ b
  }

  /**
    * Compute the viewshed for the aircraft; the craft's possible
    * locations and altitute are given as layer data and metadata.
    */
  def craft(
    sparkContext: SparkContext,
    reader: FilteringLayerReader[LayerId],
    writer: LayerWriter[LayerId],
    attributeStore: AttributeStore,
    terrainName: String,
    observerName: String,
    outputName: String,
    zoom: Int,
    maxDistance: Double,
    magic: Int
  ): Unit = {
    implicit val sc = sparkContext
    val terrainId = LayerId(terrainName, zoom)
    val _terrain = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](terrainId)
    val terrain =
      if (_terrain.partitions.length >= (1<<7)) _terrain
      else ContextRDD(_terrain.repartition((1<<7)), _terrain.metadata)
    val observerId0 = LayerId(observerName, 0)
    val observerIdZ = LayerId(observerName, zoom - magic)
    val _observer = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](observerIdZ)
    val observer =
      if (_observer.partitions.length >= (1<<7)) _observer
      else ContextRDD(_observer.repartition((1<<7)), _observer.metadata)
    val outputId = LayerId(outputName, 0)
    val altitude = -attributeStore.read[Double](observerId0, "altitude")
    val touched = attributeStore
      .read[List[SpatialKey]](observerId0, "touched")
      .map({ k => SpatialKey(k.col >> magic, k.row >> magic) })
      .toSet
    val mt = observer.metadata.mapTransform

    val points: Seq[Point6D] = {
      observer
        .filter({ case (k, _) => touched.contains(k) })
        .map({ case (k, v) =>
          val extent = mt(k)
          val cols = v.cols
          val rows = v.rows
          val re = RasterExtent(extent, cols, rows)
          val points = mutable.ArrayBuffer.empty[Point6D]

          v.foreach({ (col1, row1, z1) =>
            var boundary = false
            if (isData(z1) && (z1 > 0)) {
              for (a <- Array(-1,0,+1); b <- Array(-1,0,+1)) {
                val col2 = col1 + a
                val row2 = row1 + b
                val z2 =
                  if ((0 <= col2) && (col2 < cols) && (0 <= row2) && (row2 < rows)) v.get(col2,row2)
                  else 1
                if (!isData(z2) || z2 <= 0) boundary = true
              }
              if (boundary) {
                val (x, y) = re.gridToMap(col1, row1)
                points.append(Array(x, y, altitude, 0.0, -1.0, Double.NegativeInfinity))
              }
            }})
          points })
        .aggregate(mutable.ArrayBuffer.empty[Point6D])(combine, combine)
    }
    logger.info(s"${points.length} points")

    val before = System.currentTimeMillis
    val src = terrain.viewshed(
      points = points,
      maxDistance = maxDistance
    )
    val after1 = System.currentTimeMillis
    logger.info(s"Craft viewshed computed in ${after1 - before} ms")

    Pyramid.upLevels(src, layoutScheme, zoom, 1)({ (rdd, zoom) =>
      logger.info(s"Level $zoom craft viewshed stored")
      writer.write(LayerId(outputName, zoom), rdd, ZCurveKeyIndexMethod) })
    val after2 = System.currentTimeMillis
    val millis = after2 - before
    logger.info(s"Craft viewshed+pyramid computed in $millis ms")

    // attributeStore.write(outputId, "points", points.length)
    attributeStore.write(outputId, "millis", millis)
  }

  /**
    * Compute the viewshed for the aircraft; the craft's possible
    * locations are given as a polygon and its altitude is given as a
    * parameter.
    */
  def polygon(
    sparkContext: SparkContext,
    reader: FilteringLayerReader[LayerId],
    writer: LayerWriter[LayerId],
    attributeStore: AttributeStore,
    terrainName: String,
    geometry: MultiPolygon,
    altitude: Double,
    outputName: String,
    zoom: Int,
    maxDistance: Double
  ): Unit = {
    implicit val sc = sparkContext
    val terrainId = LayerId(terrainName, zoom)
    val _terrain = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](terrainId)
    val terrain =
      if (_terrain.partitions.length >= (1<<7)) _terrain
      else ContextRDD(_terrain.repartition((1<<7)), _terrain.metadata)
    val outputId = LayerId(outputName, 0)
    val alt = -altitude
    val b = geometry.boundary.toGeometry.get
    val re = RasterExtent(geometry.envelope, 512, 512)
    val o = Rasterizer.Options.DEFAULT

    val points = mutable.ArrayBuffer.empty[Point6D]
    Rasterizer.foreachCellByGeometry(b, re, o)({ (col, row) =>
      val (x, y) = re.gridToMap(col, row)
      points.append(Array(x, y, alt, 0.0, -1.0, Double.NegativeInfinity))
    })

    logger.info(s"${points.length} points")
    val before = System.currentTimeMillis
    val src = terrain.viewshed(
      points = points,
      maxDistance = maxDistance
    )
    val after1 = System.currentTimeMillis
    logger.info(s"Craft viewshed computed in ${after1 - before} ms")

    Pyramid.upLevels(src, layoutScheme, zoom, 1)({ (rdd, zoom) =>
      logger.info(s"Level $zoom craft viewshed stored")
      writer.write(LayerId(outputName, zoom), rdd, ZCurveKeyIndexMethod) })
    val after2 = System.currentTimeMillis
    val millis = after2 - before
    logger.info(s"Craft viewshed+pyramid computed in $millis ms")

    attributeStore.write(outputId, "millis", millis)
  }

}
