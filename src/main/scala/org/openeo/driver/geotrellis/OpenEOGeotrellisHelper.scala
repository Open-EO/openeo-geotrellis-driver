package org.openeo.driver.geotrellis

import javax.json.JsonArray

import geotrellis.raster.{DoubleCellType, MultibandTile, Tile}
import geotrellis.spark.{ContextRDD, SpaceTimeKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD

object OpenEOGeotrellisHelper {
  def  bandArithmetic(bands: JsonArray, function: String, inputRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]): ContextRDD[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]] = {

    //TODO convert bands, apply actual python function
    val converted: RDD[(SpaceTimeKey,Tile)] = inputRDD.mapValues(tile => tile.subsetBands(0, 1, 2).convert(DoubleCellType).combineDouble(0, 1) { (r, nir) =>
      (nir - r) / (nir + r)
    })
    new ContextRDD[SpaceTimeKey,Tile,TileLayerMetadata[SpaceTimeKey]](converted, inputRDD.metadata)
  }
}
