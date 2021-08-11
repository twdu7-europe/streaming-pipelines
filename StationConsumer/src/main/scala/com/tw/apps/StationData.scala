package com.tw.apps

import java.time.Instant

case class StationData(
                        bikes_available: Integer,
                        docks_available: Integer,
                        is_renting: Boolean,
                        is_returning: Boolean,
                        last_updated: String,
                        station_id: String,
                        name: String,
                        latitude: Double,
                        longitude: Double
                      )
