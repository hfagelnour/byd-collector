package com.bydcollector.collector

import android.content.Context
import android.content.SharedPreferences
import com.bydcollector.collector.mqtt.HaDiscoveryBuilder
import com.bydcollector.collector.mqtt.HaMqttConfig
import com.bydcollector.collector.mqtt.MqttCleanupCore
import com.bydcollector.collector.mqtt.MqttDestinationIdentity
import com.bydcollector.collector.mqtt.MqttPublicationGate
import com.bydcollector.collector.mqtt.MqttPublicationJournal
import com.bydcollector.collector.runtime.RuntimeJobsService
import com.bydcollector.collector.security.KeystoreSecretStore
import com.bydcollector.collector.service.CollectorSettings
import org.json.JSONArray
import org.json.JSONObject
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

/** Emulator-only, recoverable real-journal data for visually checking MQTT cleanup UI. */
internal object MqttCleanupUiFixture {
    private const val FIXTURE_PREFERENCES = "mqtt_cleanup_ui_fixture"
    private const val FIXTURE_STATE = "state"
    private const val FIXTURE_PREFIX = "ui_cleanup_"
    private const val LOOPBACK_HOST = "127.0.0.1"
    private const val LOOPBACK_PORT = 1
    private const val RETRY_ERROR = "ui_cleanup_fixture_partial_error_retry_available"
    private val settingKeys = listOf(
        CollectorSettings.KEY_MQTT_HOST,
        CollectorSettings.KEY_MQTT_PORT,
        CollectorSettings.KEY_MQTT_ALTERNATIVE_HOST,
        CollectorSettings.KEY_MQTT_ALTERNATIVE_PORT,
        CollectorSettings.KEY_MQTT_DEVICE_ID,
        CollectorSettings.KEY_MQTT_ENABLED,
        CollectorSettings.KEY_MQTT_MANUAL_STOP,
        CollectorSettings.KEY_MQTT_AUTO_START
    )

    private data class VehicleFixture(
        val id: String,
        val name: String,
        val topics: List<String>,
        val uncertainTopic: String? = null
    )

    fun run(context: Context, action: String): String = when (action) {
        "seed" -> seed(context)
        "clear" -> clear(context)
        else -> error("mqttCleanupUi must be seed or clear")
    }

    private fun seed(context: Context): String {
        val fixturePrefs = context.getSharedPreferences(FIXTURE_PREFERENCES, Context.MODE_PRIVATE)
        check(!fixturePrefs.contains(FIXTURE_STATE)) { "A cleanup UI fixture is already saved; clear it before seeding again" }
        requireLoopbackPortClosed()

        val app = context.applicationContext as BydCollectorApplication
        val journal = app.mqttPublicationJournal
        val settings = context.getSharedPreferences(CollectorSettings.PREFS_NAME, Context.MODE_PRIVATE)
        val secretStore = KeystoreSecretStore(context)
        val nonce = UUID.randomUUID().toString().replace("-", "").take(12)
        val currentId = "${FIXTURE_PREFIX}current_$nonce"
        val previousId = "${FIXTURE_PREFIX}previous_$nonce"
        val testId = "${FIXTURE_PREFIX}test_$nonce"
        val prospectiveConfig = config(settings, secretStore).copy(
            host = LOOPBACK_HOST,
            port = LOOPBACK_PORT,
            alternativeHost = null,
            alternativePort = null,
            deviceId = currentId
        )
        val scope = MqttDestinationIdentity.configuredScopes(prospectiveConfig).single()
        check(journal.cleanupGroups(setOf(scope.key), currentId).isEmpty()) {
            "The localhost fixture broker/account already has publication history; refusing to touch it"
        }
        check(MqttPublicationGate.activeSessions().none { scope.key in it.scopeKeys }) {
            "A session is already active on the localhost fixture scope"
        }
        check(activeCleanup(context, setOf(currentId, previousId, testId)).not()) {
            "A cleanup job for this fixture ID is already active"
        }

        val prefix = prospectiveConfig.normalizedDiscoveryPrefix()
        val vehicles = listOf(
            VehicleFixture(currentId, "${FIXTURE_PREFIX}current_vehicle_$nonce",
                listOf("$prefix/sensor/$currentId/battery/config")),
            VehicleFixture(previousId, "${FIXTURE_PREFIX}previous_vehicle_$nonce", listOf(
                "$prefix/sensor/$previousId/battery/config",
                "$prefix/sensor/$previousId/range/config",
                "$prefix/sensor/$previousId/door_lock/config"
            ), uncertainTopic = "$prefix/sensor/$previousId/door_lock/config"),
            VehicleFixture(testId, "${FIXTURE_PREFIX}test_vehicle_$nonce",
                listOf("$prefix/sensor/$testId/battery/config"))
        )
        val state = JSONObject()
            .put("scopeKey", scope.key)
            .put("savedSettings", snapshotSettings(settings))
            .put("vehicles", JSONArray().apply { vehicles.forEach { put(encode(it)) } })
            .put("phase", "prepared")
        check(fixturePrefs.edit().putString(FIXTURE_STATE, state.toString()).commit()) {
            "Could not persist the recoverable cleanup fixture marker"
        }

        check(settings.edit()
            .putString(CollectorSettings.KEY_MQTT_HOST, LOOPBACK_HOST)
            .putInt(CollectorSettings.KEY_MQTT_PORT, LOOPBACK_PORT)
            .remove(CollectorSettings.KEY_MQTT_ALTERNATIVE_HOST)
            .remove(CollectorSettings.KEY_MQTT_ALTERNATIVE_PORT)
            .putString(CollectorSettings.KEY_MQTT_DEVICE_ID, currentId)
            .putBoolean(CollectorSettings.KEY_MQTT_ENABLED, false)
            .putBoolean(CollectorSettings.KEY_MQTT_MANUAL_STOP, true)
            .putBoolean(CollectorSettings.KEY_MQTT_AUTO_START, false)
            .commit()) { "Could not isolate MQTT settings on the loopback fixture endpoint" }

        val fixtureConfig = config(settings, secretStore)
        val fixtureScopeKeys = MqttDestinationIdentity.configuredScopes(fixtureConfig).map { it.key }.toSet()
        check(fixtureScopeKeys == setOf(scope.key)) { "Fixture scope changed while seeding" }
        check(journal.cleanupGroups(fixtureScopeKeys, currentId).isEmpty()) {
            "The isolated fixture scope acquired unrelated history during setup"
        }

        vehicles.forEach { vehicle ->
            vehicle.topics.forEach { topic ->
                val payloadHash = sha256("${vehicle.id}:$topic:config")
                if (topic == vehicle.uncertainTopic) {
                    journal.recordDiscoveryAttempt(scope.key, vehicle.id, topic, payloadHash, vehicle.name, now())
                } else {
                    journal.recordDiscoveryAcknowledged(scope.key, vehicle.id, topic, payloadHash, vehicle.name, now(), remove = false)
                }
            }
        }

        val core = MqttCleanupCore(journal, configProvider = { fixtureConfig })
        val previous = core.groups().single { it.deviceId == previousId }
        check(previous.topicCount == 3 && previous.uncertainCount == 1)
        val operationId = journal.beginCleanup(previous.selection, fixtureScopeKeys, now())
        state.put("operationId", operationId)
        check(fixturePrefs.edit().putString(FIXTURE_STATE, state.toString()).commit()) {
            "Could not persist the cleanup retry receipt"
        }
        val targets = journal.cleanupTargets(operationId)
        check(targets.size == 3)
        val acknowledged = targets.first()
        journal.acknowledgeCleanupTarget(operationId, acknowledged.target, now())
        val failed = targets.first { it.target != acknowledged.target }
        journal.failCleanup(operationId, failed.target, RETRY_ERROR, now())
        state.put("phase", "seeded")
        check(fixturePrefs.edit().putString(FIXTURE_STATE, state.toString()).commit()) {
            "Could not finalize the cleanup fixture marker"
        }

        val groups = core.groups().filter { it.deviceId in vehicles.map(VehicleFixture::id) }
        check(groups.size == 3) { "Expected three fixture groups, got ${groups.size}" }
        val current = groups.single { it.deviceId == currentId }
        val retry = groups.single { it.deviceId == previousId }
        val test = groups.single { it.deviceId == testId }
        check(current.current && current.topicCount == 1)
        check(retry.state == "error" && retry.error == RETRY_ERROR && retry.topicCount == 3 && retry.uncertainCount == 1)
        check(!test.current && test.topicCount == 1 && test.uncertainCount == 0)
        return "SEEDED groups=${groups.size} current=1 unknown=${retry.uncertainCount} retry=1 endpoint=$LOOPBACK_HOST:$LOOPBACK_PORT"
    }

    private fun clear(context: Context): String {
        val fixturePrefs = context.getSharedPreferences(FIXTURE_PREFERENCES, Context.MODE_PRIVATE)
        val rawState = fixturePrefs.getString(FIXTURE_STATE, null) ?: return "CLEARED groups=0 (no fixture saved)"
        val state = JSONObject(rawState)
        val scopeKey = state.getString("scopeKey")
        check(scopeKey.matches(Regex("[a-f0-9]{64}"))) { "Saved fixture scope is invalid; marker retained for recovery" }
        val vehicles = decodeVehicles(state)
        val ids = vehicles.map(VehicleFixture::id).toSet()
        check(vehicles.size == 3 && ids.size == 3 && ids.all { it.startsWith(FIXTURE_PREFIX) }) {
            "Saved fixture IDs are invalid; marker retained for recovery"
        }
        check(vehicles.all { vehicle -> vehicle.topics.isNotEmpty() && vehicle.topics.all { it.contains("/${vehicle.id}/") } }) {
            "Saved fixture topics are invalid; marker retained for recovery"
        }
        check(!activeCleanup(context, ids)) { "A fixture cleanup job is still queued or running; retry clear after it finishes" }
        check(MqttPublicationGate.activeSessions().none { it.deviceId in ids && scopeKey in it.scopeKeys }) {
            "A fixture MQTT session is active; retry clear after it ends"
        }

        val journal = (context.applicationContext as BydCollectorApplication).mqttPublicationJournal
        removeFixtureRows(journal, scopeKey, vehicles, state.optString("operationId").takeIf(String::isNotBlank))
        restoreSettings(
            context.getSharedPreferences(CollectorSettings.PREFS_NAME, Context.MODE_PRIVATE),
            state.getJSONObject("savedSettings")
        )
        val leftovers = journal.cleanupGroups(setOf(scopeKey), "").count { it.deviceId in ids }
        check(leftovers == 0) { "Fixture rows remain; recovery marker retained" }
        check(fixturePrefs.edit().remove(FIXTURE_STATE).commit()) { "Fixture data is cleared, but its recovery marker remains" }
        return "CLEARED groups=3 remaining=$leftovers restoredSettings=${settingKeys.size}"
    }

    private fun activeCleanup(context: Context, ids: Set<String>): Boolean {
        val jobs = JSONArray(RuntimeJobsService.snapshot(context))
        return (0 until jobs.length()).map(jobs::getJSONObject).any {
            it.optString("kind") == "mqttCleanup" && it.optString("status") in setOf("queued", "running") &&
                it.optJSONObject("args")?.optString("deviceId")?.let(ids::contains) == true
        }
    }

    private fun config(preferences: SharedPreferences, secretStore: KeystoreSecretStore): HaMqttConfig {
        val secretUser = secretStore.read(CollectorSettings.SECRET_MQTT_USERNAME)
        val username = secretUser?.takeIf(String::isNotBlank)
            ?: preferences.getString(CollectorSettings.KEY_MQTT_USERNAME, null)?.takeIf(String::isNotBlank)
        val defaultId = HaDiscoveryBuilder.DEVICE_ID
        return HaMqttConfig(
            enabled = preferences.getBoolean(CollectorSettings.KEY_MQTT_ENABLED, false),
            discoveryEnabled = true,
            host = preferences.getString(CollectorSettings.KEY_MQTT_HOST, "") ?: "",
            port = preferences.getInt(CollectorSettings.KEY_MQTT_PORT, CollectorSettings.DEFAULT_MQTT_PORT),
            username = username,
            password = null,
            clientId = preferences.getString(CollectorSettings.KEY_MQTT_CLIENT_ID, HaMqttConfig.DEFAULT_CLIENT_ID)
                ?: HaMqttConfig.DEFAULT_CLIENT_ID,
            topicPrefix = preferences.getString(CollectorSettings.KEY_MQTT_TOPIC_PREFIX, HaMqttConfig.DEFAULT_TOPIC_PREFIX)
                ?: HaMqttConfig.DEFAULT_TOPIC_PREFIX,
            discoveryPrefix = preferences.getString(CollectorSettings.KEY_MQTT_DISCOVERY_PREFIX, HaMqttConfig.DEFAULT_DISCOVERY_PREFIX)
                ?: HaMqttConfig.DEFAULT_DISCOVERY_PREFIX,
            deviceId = preferences.getString(CollectorSettings.KEY_MQTT_DEVICE_ID, defaultId)?.ifBlank { defaultId } ?: defaultId,
            enabledCategories = emptySet(),
            alternativeHost = preferences.getString(CollectorSettings.KEY_MQTT_ALTERNATIVE_HOST, null)
                ?.trim()?.takeIf(String::isNotBlank),
            alternativePort = if (preferences.contains(CollectorSettings.KEY_MQTT_ALTERNATIVE_PORT)) {
                preferences.getInt(CollectorSettings.KEY_MQTT_ALTERNATIVE_PORT, 0)
            } else null
        )
    }

    private fun snapshotSettings(preferences: SharedPreferences): JSONObject = JSONObject().apply {
        settingKeys.forEach { key ->
            val value = preferences.all[key]
            val item = JSONObject().put("present", preferences.contains(key))
            if (preferences.contains(key)) when (value) {
                is String -> item.put("type", "string").put("value", value)
                is Int -> item.put("type", "int").put("value", value)
                is Boolean -> item.put("type", "boolean").put("value", value)
                else -> error("Unexpected setting type for $key")
            }
            put(key, item)
        }
    }

    private fun restoreSettings(preferences: SharedPreferences, snapshot: JSONObject) {
        val editor = preferences.edit()
        settingKeys.forEach { key ->
            val item = snapshot.getJSONObject(key)
            if (!item.getBoolean("present")) editor.remove(key)
            else when (item.getString("type")) {
                "string" -> editor.putString(key, item.getString("value"))
                "int" -> editor.putInt(key, item.getInt("value"))
                "boolean" -> editor.putBoolean(key, item.getBoolean("value"))
                else -> error("Saved setting type is invalid; recovery marker retained")
            }
        }
        check(editor.commit()) { "Could not restore settings; recovery marker retained" }
    }

    private fun encode(vehicle: VehicleFixture) = JSONObject()
        .put("id", vehicle.id)
        .put("name", vehicle.name)
        .put("topics", JSONArray().apply { vehicle.topics.forEach { put(it) } })
        .put("uncertainTopic", vehicle.uncertainTopic ?: JSONObject.NULL)

    private fun decodeVehicles(state: JSONObject): List<VehicleFixture> {
        val list = state.getJSONArray("vehicles")
        return (0 until list.length()).map { index ->
            val item = list.getJSONObject(index)
            val topics = item.getJSONArray("topics").let { values ->
                (0 until values.length()).map(values::getString)
            }
            val uncertainTopic = if (item.isNull("uncertainTopic")) null else item.getString("uncertainTopic")
            VehicleFixture(item.getString("id"), item.getString("name"), topics,
                uncertainTopic)
        }
    }

    private fun removeFixtureRows(
        journal: MqttPublicationJournal,
        scopeKey: String,
        vehicles: List<VehicleFixture>,
        savedOperationId: String?
    ) {
        val db = journal.writableDatabase
        db.beginTransaction()
        try {
            val operationIds = linkedSetOf<String>()
            val allTopics = vehicles.flatMap { it.topics }.distinct()
            val ids = vehicles.map { it.id }.distinct()
            val fixtureTargets = vehicles.flatMap { vehicle ->
                vehicle.topics.map { topic -> Triple(scopeKey, vehicle.id, topic) }
            }.toSet()
            if (allTopics.isNotEmpty()) {
                val idMarks = ids.joinToString(",") { "?" }
                val topicMarks = allTopics.joinToString(",") { "?" }
                db.rawQuery(
                    "SELECT DISTINCT operation_id FROM mqtt_cleanup_target WHERE scope_key = ? AND device_id IN ($idMarks) AND topic IN ($topicMarks)",
                    arrayOf(scopeKey, *ids.toTypedArray(), *allTopics.toTypedArray())
                ).use { cursor -> while (cursor.moveToNext()) operationIds += cursor.getString(0) }
            }
            savedOperationId?.let { id ->
                db.rawQuery("SELECT device_id FROM mqtt_cleanup_operation WHERE id = ?", arrayOf(id)).use { cursor ->
                    if (cursor.moveToFirst()) {
                        check(cursor.getString(0) in ids) { "Saved cleanup operation is outside the fixture; marker retained" }
                        operationIds += id
                    }
                }
            }
            operationIds.forEach { operationId ->
                db.rawQuery("SELECT device_id FROM mqtt_cleanup_operation WHERE id = ?", arrayOf(operationId)).use { cursor ->
                    check(cursor.moveToFirst() && cursor.getString(0) in ids) {
                        "Cleanup operation is outside the fixture; marker retained"
                    }
                }
                db.rawQuery(
                    "SELECT scope_key, device_id, topic FROM mqtt_cleanup_target WHERE operation_id = ?",
                    arrayOf(operationId)
                ).use { cursor ->
                    while (cursor.moveToNext()) {
                        check(Triple(cursor.getString(0), cursor.getString(1), cursor.getString(2)) in fixtureTargets) {
                            "Cleanup operation includes a non-fixture target; marker retained"
                        }
                    }
                }
                db.delete("mqtt_cleanup_operation", "id = ?", arrayOf(operationId))
            }
            vehicles.forEach { vehicle ->
                vehicle.topics.forEach { topic ->
                    val where = "scope_key = ? AND device_id = ? AND topic = ?"
                    val args = arrayOf(scopeKey, vehicle.id, topic)
                    db.delete("mqtt_cleanup_target", where, args)
                    db.delete("mqtt_discovery_history", where, args)
                }
                db.delete("mqtt_destination_epoch", "scope_key = ? AND device_id = ?", arrayOf(scopeKey, vehicle.id))
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }

    private fun sha256(value: String): String = MessageDigest.getInstance("SHA-256")
        .digest(value.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }

    private fun now(): String = Instant.now().toString()

    private fun requireLoopbackPortClosed() {
        val connection = runCatching {
            java.net.Socket().use { it.connect(java.net.InetSocketAddress(LOOPBACK_HOST, LOOPBACK_PORT), 500) }
        }
        check(connection.isFailure) { "127.0.0.1:1 is already accepting connections; fixture was not seeded" }
    }
}
