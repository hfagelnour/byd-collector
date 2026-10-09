package com.bydcollector.collector.mqtt

import com.bydcollector.collector.data.normalized.NormalizedFieldCatalog
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import org.json.JSONObject

class HaDiscoveryBuilderTest {
    @Test
    fun customCarsHaveSeparateIdentityAndNameModelEditsKeepItStable() {
        val first = config(topicPrefix = "cars/first").copy(deviceId = "first_car", deviceIdentifier = "first-device",
            deviceName = "First car", deviceModel = "First model")
        val second = first.copy(deviceId = "second_car", deviceIdentifier = "second-device", topicPrefix = "cars/second")
        fun build(config: HaMqttConfig) = HaDiscoveryBuilder.discoveryMessages(config, listOf(NormalizedFieldCatalog.soc)).single()
        val firstMessage = build(first)
        val firstPayload = JSONObject(firstMessage.payload)
        val secondMessage = build(second)
        assertEquals("homeassistant/sensor/first_car/soc/config", firstMessage.topic)
        assertEquals("first_car_soc", firstPayload.getString("unique_id"))
        assertEquals("cars/first/state/battery", firstPayload.getString("state_topic"))
        val device = firstPayload.getJSONObject("device")
        assertEquals("first-device", device.getJSONArray("identifiers").getString(0))
        assertEquals("First car", device.getString("name"))
        assertEquals("First model", device.getString("model"))
        assertFalse(firstMessage.topic == secondMessage.topic)
        assertEquals("second_car_soc", JSONObject(secondMessage.payload).getString("unique_id"))

        val renamed = build(first.copy(deviceName = "Renamed", deviceModel = "New model"))
        assertEquals(firstMessage.topic, renamed.topic)
        val renamedPayload = JSONObject(renamed.payload)
        assertEquals(firstPayload.getString("unique_id"), renamedPayload.getString("unique_id"))
        assertEquals(device.getJSONArray("identifiers").toString(), renamedPayload.getJSONObject("device").getJSONArray("identifiers").toString())
    }

    @Test
    fun invalidIdsCannotProduceDiscoveryAndLegacyTombstoneFollowsCustomId() {
        assertFailsWith<IllegalArgumentException> { HaDiscoveryBuilder.discoveryMessages(config().copy(deviceId = "bad/id")) }
        val custom = config(enabledCategories = setOf("body")).copy(deviceId = "my_car")
        val messages = HaDiscoveryBuilder.discoveryMessages(custom, listOf(NormalizedFieldCatalog.sunroofPosition))
        assertEquals("homeassistant/binary_sensor/my_car/bodywork_sunroof_windoblind_position/config", messages.first().topic)
        assertEquals("", messages.first().payload)
        assertTrue(messages.all { "/my_car/" in it.topic })
        val blank = HaDiscoveryBuilder.discoveryMessages(config().copy(deviceId = ""), listOf(NormalizedFieldCatalog.soc)).single()
        assertEquals("homeassistant/sensor/byd_sealion_07/soc/config", blank.topic)
    }

    @Test
    fun discoveryUsesGroupedCategoryStateTopicForSoc() {
        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(),
            fields = listOf(NormalizedFieldCatalog.soc)
        )

        val message = messages.single()
        assertEquals("homeassistant/sensor/byd_sealion_07/soc/config", message.topic)
        assertTrue(message.retained)
        assertEquals(1, message.qos)

        val json = JSONObject(message.payload)
        assertEquals("Battery SOC display", json.getString("name"))
        assertEquals("byd_sealion_07_soc", json.getString("unique_id"))
        assertEquals("bydcollector/state/battery", json.getString("state_topic"))
        assertEquals("bydcollector/status", json.getString("availability_topic"))
        assertEquals("{{ value_json.availability }}", json.getString("availability_template"))
        assertEquals("online", json.getString("payload_available"))
        assertEquals("offline", json.getString("payload_not_available"))
        assertEquals("{{ value_json.fields.soc }}", json.getString("value_template"))
        assertEquals("battery", json.getString("device_class"))
        assertEquals("measurement", json.getString("state_class"))
        assertEquals("%", json.getString("unit_of_measurement"))
        assertEquals(
            HaDiscoveryBuilder.DEVICE_IDENTIFIER,
            json.getJSONObject("device").getJSONArray("identifiers").getString(0)
        )
    }

    @Test
    fun discoveryPublishesInternalSocAndEnergyWithExactStateClasses() {
        val fieldsByKey = NormalizedFieldCatalog.fields.associateBy { it.fieldKey }
        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(),
            fields = listOf(
                fieldsByKey.getValue("soc_internal"),
                fieldsByKey.getValue("battery_remaining_energy_kwh"),
                fieldsByKey.getValue("trip_energy_kwh"),
                fieldsByKey.getValue("cumulative_energy_kwh")
            )
        ).associateBy { it.topic.substringBeforeLast("/config").substringAfterLast('/') }

        assertEquals(4, messages.size)
        val soc = JSONObject(messages.getValue("soc_internal").payload)
        assertEquals("battery", soc.getString("device_class"))
        assertEquals("measurement", soc.getString("state_class"))
        assertEquals("%", soc.getString("unit_of_measurement"))

        val remaining = JSONObject(messages.getValue("battery_remaining_energy_kwh").payload)
        assertEquals("energy_storage", remaining.getString("device_class"))
        assertEquals("measurement", remaining.getString("state_class"))
        assertEquals("kWh", remaining.getString("unit_of_measurement"))

        val trip = JSONObject(messages.getValue("trip_energy_kwh").payload)
        assertEquals("energy", trip.getString("device_class"))
        assertFalse(trip.has("state_class"))

        val cumulative = JSONObject(messages.getValue("cumulative_energy_kwh").payload)
        assertEquals("energy", cumulative.getString("device_class"))
        assertEquals("total", cumulative.getString("state_class"))
    }

    @Test
    fun discoverySkipsDisabledCategories() {
        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(enabledCategories = setOf("motion")),
            fields = listOf(NormalizedFieldCatalog.soc, NormalizedFieldCatalog.speedKmh)
        )

        assertEquals(1, messages.size)
        assertTrue(messages.single().topic.contains("/speed_kmh/"))
        assertFalse(messages.single().topic.contains("/soc/"))
    }

    @Test
    fun discoveryNormalizesBlankPrefixes() {
        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(topicPrefix = "///", discoveryPrefix = ""),
            fields = listOf(NormalizedFieldCatalog.soc)
        )

        val message = messages.single()
        assertEquals("homeassistant/sensor/byd_sealion_07/soc/config", message.topic)
        assertEquals(
            "bydcollector/state/battery",
            JSONObject(message.payload).getString("state_topic")
        )
    }

    @Test
    fun binarySensorDiscoveryUsesStableBooleanPayloadMapping() {
        val message = HaDiscoveryBuilder.discoveryMessages(
            config = config(enabledCategories = setOf("body")),
            fields = listOf(NormalizedFieldCatalog.driverDoor)
        ).single()

        assertEquals("homeassistant/binary_sensor/byd_sealion_07/driver_door_open/config", message.topic)
        val json = JSONObject(message.payload)
        assertEquals("true", json.getString("payload_on"))
        assertEquals("false", json.getString("payload_off"))
        assertEquals(
            "{{ 'true' if value_json.fields.driver_door_open is true else 'false' if value_json.fields.driver_door_open is false else '' }}",
            json.getString("value_template")
        )
        assertEquals("door", json.getString("device_class"))
    }

    @Test
    fun sunroofDiscoveryTombstonesLegacyBinaryAndPublishesUnitlessSensor() {
        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(enabledCategories = setOf("body")),
            fields = listOf(NormalizedFieldCatalog.sunroofPosition)
        )

        assertEquals(2, messages.size)
        assertEquals(
            "homeassistant/binary_sensor/byd_sealion_07/bodywork_sunroof_windoblind_position/config",
            messages[0].topic
        )
        assertEquals("", messages[0].payload)
        assertTrue(messages[0].retained)
        assertEquals(1, messages[0].qos)

        val sensor = messages[1]
        assertEquals(
            "homeassistant/sensor/byd_sealion_07/bodywork_sunroof_windoblind_position/config",
            sensor.topic
        )
        val json = JSONObject(sensor.payload)
        assertEquals("byd_sealion_07_bodywork_sunroof_windoblind_position", json.getString("unique_id"))
        assertEquals("bydcollector/state/body", json.getString("state_topic"))
        assertEquals(
            "{{ value_json.fields.bodywork_sunroof_windoblind_position }}",
            json.getString("value_template")
        )
        assertFalse(json.has("unit_of_measurement"))
        assertFalse(json.has("device_class"))
        assertFalse(json.has("state_class"))
        assertFalse(json.has("payload_on"))
        assertFalse(json.has("payload_off"))
    }

    @Test
    fun rawDischargeDiscoveryOmitsPowerSemantics() {
        val message = HaDiscoveryBuilder.discoveryMessages(
            config = config(enabledCategories = setOf("battery")),
            fields = listOf(NormalizedFieldCatalog.maxDischargePowerAllow)
        ).single()

        assertEquals(
            "homeassistant/sensor/byd_sealion_07/max_discharge_power_allow_raw/config",
            message.topic
        )
        val json = JSONObject(message.payload)
        assertFalse(json.has("unit_of_measurement"))
        assertFalse(json.has("device_class"))
        assertFalse(json.has("state_class"))
        assertEquals("{{ value_json.fields.max_discharge_power_allow_raw }}", json.getString("value_template"))
    }

    @Test
    fun dischargeLimitDiscoveryKeepsRawEntityAndAddsSeparatePowerLimit() {
        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(enabledCategories = setOf("battery")),
            fields = listOf(NormalizedFieldCatalog.maxDischargePowerAllow, NormalizedFieldCatalog.maxDischargePowerAllowKw)
        )
        assertEquals(2, messages.size)
        val limit = JSONObject(messages.single { it.topic.endsWith("/max_discharge_power_allow_kw/config") }.payload)
        assertEquals("Discharge power limit", limit.getString("name"))
        assertEquals("kW", limit.getString("unit_of_measurement"))
        assertEquals("power", limit.getString("device_class"))
        assertEquals("measurement", limit.getString("state_class"))
        assertEquals("{{ value_json.fields.max_discharge_power_allow_kw }}", limit.getString("value_template"))
        assertFalse(JSONObject(messages.single { it.topic.endsWith("/max_discharge_power_allow_raw/config") }.payload).has("unit_of_measurement"))
    }

    @Test
    fun discoverySkipsNonDefaultRawFieldsWithinEnabledDefaultCategory() {
        val tireStateRaw = NormalizedFieldCatalog.fields.single { it.fieldKey == "tyre_state_lf" }
        val radarDistance = NormalizedFieldCatalog.fields.single { it.fieldKey == "radar_1025_neg_1728053151_5" }

        val messages = HaDiscoveryBuilder.discoveryMessages(
            config = config(enabledCategories = setOf("safety", "battery")),
            fields = listOf(tireStateRaw, radarDistance)
        )

        assertEquals(emptyList(), messages)
    }

    @Test
    fun curatedCatalogDoesNotAdvertiseRemovedLegacyDiscoveryFields() {
        val removed = setOf(
            "adas_acc_mode_raw",
            "total_elec_consumption_kwh",
            "trip_elec_consumption_kwh",
            "charging_type_raw"
        )
        val fields = NormalizedFieldCatalog.fields.map { it.fieldKey }.toSet()

        assertEquals(emptySet(), removed.intersect(fields))
    }

    private fun config(
        topicPrefix: String = HaMqttConfig.DEFAULT_TOPIC_PREFIX,
        discoveryPrefix: String = HaMqttConfig.DEFAULT_DISCOVERY_PREFIX,
        enabledCategories: Set<String> = HaMqttConfig.DEFAULT_CATEGORIES
    ): HaMqttConfig {
        return HaMqttConfig(
            enabled = true,
            discoveryEnabled = true,
            host = "mqtt.local",
            port = 1883,
            username = null,
            password = null,
            clientId = HaMqttConfig.DEFAULT_CLIENT_ID,
            topicPrefix = topicPrefix,
            discoveryPrefix = discoveryPrefix,
            enabledCategories = enabledCategories
        )
    }
}
