package com.bydcollector.collector.influx

import com.bydcollector.collector.ha.HaEndpointProfile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class InfluxConfigTest {
    @Test
    fun partialAlternativePairIsRejected() {
        assertNotNull(config(alternativeHost = "influx-alt.local").validateEndpoints())
        assertNotNull(config(alternativePort = 8087).validateEndpoints())
    }

    @Test
    fun selectedEndpointClearsAlternativePair() {
        val selected = config(alternativeHost = "influx-alt.local", alternativePort = 8087)
            .copy(vehicleId = "custom car,ev=2")
            .forProfile(HaEndpointProfile.ALTERNATIVE)

        assertEquals("influx-alt.local", selected.host)
        assertEquals(8087, selected.port)
        assertEquals("custom car,ev=2", selected.vehicleId)
        assertNull(selected.alternativeHost)
        assertNull(selected.alternativePort)
    }

    @Test
    fun vehicleIdUsesSeaLionDefaultWhenBlank() {
        assertEquals(InfluxConfig.DEFAULT_VEHICLE_ID, config().vehicleId)
        assertEquals(InfluxConfig.DEFAULT_VEHICLE_ID, config().copy(vehicleId = "").normalizedVehicleId())
        assertNull(config().copy(vehicleId = " ").validateVehicleId())
    }

    @Test
    fun vehicleIdRejectsLineBreaksAndNul() {
        listOf("bad\r", "bad\n", "bad\u0000").forEach { value ->
            assertNotNull(config().copy(vehicleId = value).validateVehicleId())
            assertEquals(false, InfluxConfig.isValidVehicleId(value))
        }
    }

    @Test
    fun whitespaceAndSlashHostsAreInvalid() {
        assertNotNull(config(host = "influx host").validateEndpoints())
        assertNotNull(config(host = "http://influx").validateEndpoints())
    }

    private fun config(
        host: String = "influx.local",
        alternativeHost: String? = null,
        alternativePort: Int? = null,
        vehicleId: String = InfluxConfig.DEFAULT_VEHICLE_ID
    ) = InfluxConfig(
        enabled = true,
        host = host,
        port = 8086,
        database = "bydcollector",
        username = null,
        password = null,
        measurement = "byd_state",
        enabledCategories = setOf("battery"),
        alternativeHost = alternativeHost,
        alternativePort = alternativePort,
        vehicleId = vehicleId
    )
}
