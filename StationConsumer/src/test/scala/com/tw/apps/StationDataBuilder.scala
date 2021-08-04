package com.tw.apps

object StationDataBuilder {
  def buildStationRow(stationId: String, latitude: String = "40.68382604", longitude: String = "-73.97632328", bikesAvailable: Int = 19): String = {
    s"""
       {
          "station_id":"${stationId}",
          "bikes_available":${bikesAvailable},
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":${latitude},
          "longitude":${longitude}
       }
    """
  }
}
