package com.bydcollector.collector

import android.app.Instrumentation
import android.content.Intent
import android.os.Bundle
import android.os.ParcelFileDescriptor
import com.bydcollector.collector.mqtt.MqttCleanupGroup
import com.bydcollector.collector.mqtt.MqttDestinationIdentity
import com.bydcollector.collector.runtime.RuntimeEndpoint
import com.bydcollector.collector.runtime.RuntimeJobsService
import com.bydcollector.collector.security.KeystoreSecretStore
import com.bydcollector.collector.service.CollectorSettings
import org.json.JSONArray
import org.json.JSONObject
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/** Focused owner/UI IPC gate for durable, exact-topic MQTT cleanup jobs. */
internal object MqttCleanupRuntimeGate {
    fun run(test: Instrumentation): String {
        val context = test.targetContext
        val settings = CollectorSettings(context)
        check(!settings.isUserShutdownRequested()) { "Explicitly reopen the app before this gate" }

        val preferences = context.getSharedPreferences(CollectorSettings.PREFS_NAME, 0)
        val touchedKeys = setOf(
            CollectorSettings.KEY_MQTT_ENABLED,
            CollectorSettings.KEY_MQTT_MANUAL_STOP,
            CollectorSettings.KEY_MQTT_AUTO_START,
            CollectorSettings.KEY_MQTT_HOST,
            CollectorSettings.KEY_MQTT_PORT,
            CollectorSettings.KEY_MQTT_ALTERNATIVE_HOST,
            CollectorSettings.KEY_MQTT_ALTERNATIVE_PORT,
            CollectorSettings.KEY_MQTT_DEVICE_ID,
            CollectorSettings.KEY_MQTT_USERNAME,
            CollectorSettings.KEY_MQTT_PASSWORD,
            CollectorSettings.KEY_INFLUX_VEHICLE_ID
        )
        val savedSettings = touchedKeys.associateWith { key -> preferences.contains(key) to preferences.all[key] }
        val secretStore = KeystoreSecretStore(context)
        val savedUsername = secretStore.read(CollectorSettings.SECRET_MQTT_USERNAME)
        val savedPassword = secretStore.read(CollectorSettings.SECRET_MQTT_PASSWORD)
        val nonce = UUID.randomUUID().toString().replace("-", "").take(10)
        val currentId = "cleanup_current_$nonce"
        val oldId = "cleanup_old_$nonce"
        val otherId = "cleanup_other_$nonce"
        val oldTopics = listOf(
            "homeassistant/sensor/$oldId/battery/config",
            "homeassistant/sensor/$oldId/range/config"
        )
        val currentTopic = "homeassistant/sensor/$currentId/battery/config"
        var jobId: String? = null
        var wasUiRunning = false
        var launchedUi = false
        var forceStoppedUi = false
        val broker = LoopbackMqttBroker()
        val app = context.applicationContext as BydCollectorApplication
        val journal = app.mqttPublicationJournal
        var scopeKey: String? = null

        fun shell(command: String): String = ParcelFileDescriptor.AutoCloseInputStream(
            test.uiAutomation.executeShellCommand(command)
        ).bufferedReader().use { it.readText() }

        fun endpointCall(method: String, extras: Bundle = Bundle()): Bundle {
            extras.putInt("protocol", RuntimeEndpoint.PROTOCOL)
            extras.putString("revision", BuildConfig.RUNTIME_REVISION)
            return requireNotNull(context.contentResolver.call(RuntimeEndpoint.uri, method, null, extras))
        }

        fun edit(values: Bundle): Bundle = endpointCall("preferences.edit", Bundle().apply {
            putBundle("changes", values)
            putStringArrayList("remove", arrayListOf())
        })

        fun queryGroups(): List<MqttCleanupGroup> {
            val args = Bundle().apply {
                putInt("protocol", RuntimeEndpoint.PROTOCOL)
                putString("revision", BuildConfig.RUNTIME_REVISION)
                putString("operation", "mqttCleanupGroups")
            }
            val descriptor = requireNotNull(context.contentResolver.openTypedAssetFileDescriptor(
                RuntimeEndpoint.uri, "application/vnd.bydcollector.query", args
            ))
            return descriptor.use { fd ->
                val value = java.io.ObjectInputStream(fd.createInputStream()).use { it.readObject() }
                fd.parcelFileDescriptor.checkError()
                check(value is List<*> && value.all { it is MqttCleanupGroup }) {
                    "Cleanup query did not serialize its group model"
                }
                value.filterIsInstance<MqttCleanupGroup>()
            }
        }

        fun job(id: String): JSONObject? {
            val jobs = JSONArray(RuntimeJobsService.snapshot(context))
            return (0 until jobs.length()).map(jobs::getJSONObject).firstOrNull { it.optString("id") == id }
        }

        fun awaitJob(id: String, status: String): JSONObject {
            val deadline = android.os.SystemClock.elapsedRealtime() + 12_000L
            while (android.os.SystemClock.elapsedRealtime() < deadline) {
                job(id)?.takeIf { it.optString("status") == status }?.let { return it }
                job(id)?.let { current ->
                    check(current.optString("status") !in setOf("error", "complete") || current.optString("status") == status) {
                        "MQTT cleanup ended as ${current.optString("status")}: ${current.optString("error")}"
                    }
                }
                Thread.sleep(30L)
            }
            error("MQTT cleanup job $id did not reach $status")
        }

        fun launchUi() {
            context.startActivity(Intent().setClassName(RuntimeEndpoint.UI_PACKAGE, "com.bydcollector.collector.MainActivity")
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
            val deadline = android.os.SystemClock.elapsedRealtime() + 8_000L
            while (android.os.SystemClock.elapsedRealtime() < deadline) {
                if (shell("pidof ${RuntimeEndpoint.UI_PACKAGE}").trim().isNotEmpty()) return
                Thread.sleep(50L)
            }
            error("Collector UI process did not start")
        }

        try {
            check(RuntimeJobsService.snapshot(context).let { json ->
                val jobs = JSONArray(json)
                (0 until jobs.length()).map(jobs::getJSONObject).none {
                    it.optString("kind") == "mqttCleanup" && it.optString("status") in setOf("queued", "running")
                }
            }) { "Another MQTT cleanup job is active on this emulator" }

            check(preferences.edit()
                .putBoolean(CollectorSettings.KEY_MQTT_ENABLED, true)
                .putBoolean(CollectorSettings.KEY_MQTT_MANUAL_STOP, true)
                .putBoolean(CollectorSettings.KEY_MQTT_AUTO_START, false)
                .putString(CollectorSettings.KEY_MQTT_HOST, "127.0.0.1")
                .putInt(CollectorSettings.KEY_MQTT_PORT, broker.port)
                .remove(CollectorSettings.KEY_MQTT_ALTERNATIVE_HOST)
                .remove(CollectorSettings.KEY_MQTT_ALTERNATIVE_PORT)
                .putString(CollectorSettings.KEY_MQTT_DEVICE_ID, currentId)
                .commit()) { "Could not configure isolated loopback cleanup fixture" }
            val config = settings.mqttConfig()
            scopeKey = MqttDestinationIdentity.configuredScopes(config).single().key
            oldTopics.forEachIndexed { index, topic ->
                if (index == 0) journal.recordDiscoveryAcknowledged(scopeKey!!, oldId, topic, "ack_$nonce", "Test vehicle", "2026-10-09T00:00:00Z", remove = false)
                else journal.recordDiscoveryAttempt(scopeKey!!, oldId, topic, "uncertain_$nonce", "Test vehicle", "2026-10-09T00:00:01Z")
            }
            journal.recordDiscoveryAcknowledged(scopeKey!!, currentId, currentTopic, "current_$nonce", "Current test vehicle", "2026-10-09T00:00:02Z", remove = false)

            val groups = queryGroups()
            val selected = groups.single { it.deviceId == oldId }
            check(selected.topicCount == 2 && selected.uncertainCount == 1 && !selected.current) {
                "Serialized cleanup group did not preserve its exact publication history"
            }
            check(groups.single { it.deviceId == currentId }.current)

            fun cleanupArgs(deviceId: String, fingerprint: String) = JSONObject()
                .put("deviceId", deviceId).put("fingerprint", fingerprint)
            fun submitCleanup(args: JSONObject): String = requireNotNull(endpointCall("job.submit", Bundle().apply {
                putString("kind", "mqttCleanup")
                putString("args", args.toString())
            }).getString("id"))
            check(runCatching { submitCleanup(cleanupArgs("bad/id", selected.selection.fingerprint)) }.isFailure)
            check(runCatching { submitCleanup(cleanupArgs(oldId, "not-a-sha256")) }.isFailure)
            check(runCatching {
                submitCleanup(cleanupArgs(oldId, selected.selection.fingerprint).put("extra", true))
            }.isFailure)

            wasUiRunning = shell("pidof ${RuntimeEndpoint.UI_PACKAGE}").trim().isNotEmpty()
            if (!wasUiRunning) {
                launchUi()
                launchedUi = true
            }

            val submitted = submitCleanup(cleanupArgs(oldId, selected.selection.fingerprint))
            jobId = submitted
            broker.awaitConnect()
            check(submitCleanup(cleanupArgs(oldId, selected.selection.fingerprint)) == submitted) {
                "An active cleanup selection was not deduplicated"
            }
            check(runCatching {
                submitCleanup(cleanupArgs(otherId, selected.selection.fingerprint))
            }.isFailure) { "A different active cleanup selection was accepted" }

            val originalHost = settings.mqttHost()
            check(runCatching {
                edit(Bundle().apply { putString(CollectorSettings.KEY_MQTT_HOST, "changed-during-cleanup") })
            }.isFailure) { "MQTT endpoint edit was not blocked during cleanup" }
            check(settings.mqttHost() == originalHost)
            val passwordRead = endpointCall("secret.read", Bundle().apply {
                putString("name", CollectorSettings.SECRET_MQTT_PASSWORD)
            })
                .getString("value")
            check(passwordRead == savedPassword) { "Secret reads must remain available during cleanup" }
            check(runCatching {
                endpointCall("secret.write", Bundle().apply {
                    putString("name", CollectorSettings.SECRET_MQTT_PASSWORD)
                    putString("value", "blocked-$nonce")
                })
            }.isFailure) { "MQTT secret writes were not blocked during cleanup" }
            check(runCatching {
                endpointCall("secret.clear", Bundle().apply {
                    putString("name", CollectorSettings.SECRET_MQTT_PASSWORD)
                })
            }.isFailure) { "MQTT secret clears were not blocked during cleanup" }
            check(edit(Bundle().apply { putString(CollectorSettings.KEY_INFLUX_VEHICLE_ID, "test_vehicle_$nonce") })
                .getBoolean("committed")) { "Influx settings should remain editable" }
            check(edit(Bundle().apply { putBoolean(CollectorSettings.KEY_MQTT_ENABLED, false) }).getBoolean("committed")) {
                "Stop/disable must remain usable during cleanup"
            }

            val firstPublish = broker.awaitFirstPublish()
            shell("am force-stop ${RuntimeEndpoint.UI_PACKAGE}")
            forceStoppedUi = true
            val stopDeadline = android.os.SystemClock.elapsedRealtime() + 5_000L
            while (android.os.SystemClock.elapsedRealtime() < stopDeadline &&
                shell("pidof ${RuntimeEndpoint.UI_PACKAGE}").trim().isNotEmpty()) Thread.sleep(40L)
            check(shell("pidof ${RuntimeEndpoint.UI_PACKAGE}").trim().isEmpty()) { "UI force-stop did not finish" }
            broker.releaseFirstPublishAck()
            check(firstPublish.qos == 1 && firstPublish.retained && firstPublish.payload.isEmpty()) {
                "Cleanup did not publish an empty retained QoS 1 tombstone"
            }
            check(firstPublish.topic in oldTopics) { "Cleanup published an unselected discovery topic" }
            val completed = awaitJob(submitted, "complete")
            val receipt = completed.getJSONObject("result")
            check(receipt.getString("jobId") == submitted && receipt.getString("deviceId") == oldId &&
                receipt.getString("fingerprint") == selected.selection.fingerprint && receipt.getBoolean("acknowledged")) {
                "Completed cleanup job omitted its selection receipt"
            }
            val operationId = receipt.getString("operationId")
            check(journal.completedCleanupOperation(selected.selection) == operationId) {
                "Runtime result does not carry the durable cleanup receipt"
            }
            check(queryGroups().none { it.deviceId == oldId }) { "A fully acknowledged cleanup group remained visible" }
            val allPublished = broker.publishedPackets()
            check(allPublished.map { it.topic }.toSet() == oldTopics.toSet() &&
                allPublished.all { it.qos == 1 && it.retained && it.payload.isEmpty() }) {
                "Cleanup touched an unselected topic or did not ACK every empty retained QoS 1 tombstone"
            }

            val currentGroup = queryGroups().single { it.deviceId == currentId }
            val rejected = submitCleanup(cleanupArgs(currentId, currentGroup.selection.fingerprint))
            val failed = awaitJob(rejected, "error")
            check(failed.optString("error").contains("mqtt_cleanup_current_id")) {
                "Core rejection was incorrectly reported as a successful job"
            }
            check(queryGroups().any { it.deviceId == currentId }) { "Failed cleanup must retain its group" }
            check(edit(Bundle().apply { putString(CollectorSettings.KEY_MQTT_HOST, originalHost) }).getBoolean("committed")) {
                "MQTT settings did not become editable after a terminal cleanup error"
            }

            if (wasUiRunning) launchUi()
            return "PASS: serializable owner query, malformed args, exact active-job dedupe, endpoint/credential guard with Stop and Influx edits allowed, Paho QoS1 retained tombstones, and UI force-stop durability"
        } finally {
            broker.releaseConnect()
            broker.releaseFirstPublishAck()
            jobId?.let { id ->
                val deadline = android.os.SystemClock.elapsedRealtime() + 4_000L
                while (android.os.SystemClock.elapsedRealtime() < deadline && job(id)?.optString("status") in setOf("queued", "running")) {
                    Thread.sleep(40L)
                }
            }
            broker.close()
            scopeKey?.let { scope ->
                oldTopics.forEach { journal.recordDiscoveryAcknowledged(scope, oldId, it, "", "", "2026-10-09T00:00:10Z", remove = true) }
                journal.recordDiscoveryAcknowledged(scope, currentId, currentTopic, "", "", "2026-10-09T00:00:10Z", remove = true)
            }
            val restore = preferences.edit()
            savedSettings.forEach { (key, saved) ->
                if (!saved.first) restore.remove(key)
                else when (val value = saved.second) {
                    is String -> restore.putString(key, value)
                    is Boolean -> restore.putBoolean(key, value)
                    is Int -> restore.putInt(key, value)
                    is Long -> restore.putLong(key, value)
                    is Float -> restore.putFloat(key, value)
                    is Set<*> -> restore.putStringSet(key, value.filterIsInstance<String>().toSet())
                }
            }
            check(restore.commit()) { "Could not restore settings after cleanup gate" }
            if (savedUsername == null) secretStore.clear(CollectorSettings.SECRET_MQTT_USERNAME)
            else secretStore.write(CollectorSettings.SECRET_MQTT_USERNAME, savedUsername)
            if (savedPassword == null) secretStore.clear(CollectorSettings.SECRET_MQTT_PASSWORD)
            else secretStore.write(CollectorSettings.SECRET_MQTT_PASSWORD, savedPassword)
            if (launchedUi && !wasUiRunning && shell("pidof ${RuntimeEndpoint.UI_PACKAGE}").trim().isNotEmpty()) {
                shell("am force-stop ${RuntimeEndpoint.UI_PACKAGE}")
            }
            if (wasUiRunning && forceStoppedUi && shell("pidof ${RuntimeEndpoint.UI_PACKAGE}").trim().isEmpty()) launchUi()
        }
    }

    private data class PublishedPacket(val topic: String, val payload: String, val qos: Int, val retained: Boolean)

    private class LoopbackMqttBroker : Closeable {
        private val server = ServerSocket(0, 2, InetAddress.getByName("127.0.0.1"))
        val port: Int get() = server.localPort
        private val connectSeen = CountDownLatch(1)
        private val allowConnect = CountDownLatch(1)
        private val firstPublishSeen = CountDownLatch(1)
        private val allowFirstAck = CountDownLatch(1)
        private val firstPublish = java.util.concurrent.atomic.AtomicBoolean(true)
        private val packets = ArrayBlockingQueue<PublishedPacket>(16)
        @Volatile private var activeSocket: Socket? = null
        private val serverThread = thread(name = "mqtt-cleanup-loopback", isDaemon = true) {
            try {
                server.accept().use { socket ->
                    activeSocket = socket
                    socket.soTimeout = 15_000
                    val input = DataInputStream(socket.getInputStream())
                    val output = DataOutputStream(socket.getOutputStream())
                    val connect = readPacket(input) ?: error("MQTT client closed before CONNECT")
                    check(connect.header ushr 4 == 1) { "Expected MQTT CONNECT" }
                    connectSeen.countDown()
                    check(allowConnect.await(10, TimeUnit.SECONDS)) { "Loopback broker CONNECT gate timed out" }
                    output.write(byteArrayOf(0x20, 0x02, 0x00, 0x00))
                    output.flush()
                    while (!socket.isClosed) {
                        val packet = readPacket(input) ?: break
                        when (packet.header ushr 4) {
                            3 -> {
                                val published = parsePublish(packet)
                                packets.put(published)
                                if (firstPublish.compareAndSet(true, false)) {
                                    firstPublishSeen.countDown()
                                    check(allowFirstAck.await(10, TimeUnit.SECONDS)) { "Loopback broker PUBACK gate timed out" }
                                }
                                if (published.qos == 1) {
                                    val body = packet.body
                                    val topicSize = ((body[0].toInt() and 0xff) shl 8) or (body[1].toInt() and 0xff)
                                    val packetIdOffset = 2 + topicSize
                                    val packetId = ((body[packetIdOffset].toInt() and 0xff) shl 8) or
                                        (body[packetIdOffset + 1].toInt() and 0xff)
                                    output.write(byteArrayOf(0x40, 0x02, (packetId ushr 8).toByte(), packetId.toByte()))
                                    output.flush()
                                }
                            }
                            12 -> { output.write(byteArrayOf(0xD0.toByte(), 0x00)); output.flush() }
                            14 -> break
                        }
                    }
                }
            } catch (_: Exception) {
                if (!server.isClosed) connectSeen.countDown()
            }
        }

        fun awaitConnect() {
            check(connectSeen.await(10, TimeUnit.SECONDS)) { "Paho did not connect to the loopback broker" }
            check(activeSocket?.isClosed == false) { "Loopback MQTT connection closed before the job was observed" }
        }

        fun awaitFirstPublish(): PublishedPacket {
            allowConnect.countDown()
            check(firstPublishSeen.await(10, TimeUnit.SECONDS)) { "Paho cleanup did not publish a tombstone" }
            return requireNotNull(packets.peek())
        }

        fun releaseConnect() { allowConnect.countDown() }
        fun releaseFirstPublishAck() { allowFirstAck.countDown() }
        fun publishedPackets(): List<PublishedPacket> = packets.toList()

        override fun close() {
            allowConnect.countDown()
            allowFirstAck.countDown()
            activeSocket?.close()
            server.close()
            serverThread.join(2_000)
            check(!serverThread.isAlive) { "Loopback MQTT broker did not stop" }
        }

        private fun parsePublish(packet: Packet): PublishedPacket {
            val body = packet.body
            val topicSize = ((body[0].toInt() and 0xff) shl 8) or (body[1].toInt() and 0xff)
            val topicEnd = 2 + topicSize
            val topic = body.copyOfRange(2, topicEnd).toString(Charsets.UTF_8)
            val qos = (packet.header ushr 1) and 0x03
            val payloadOffset = topicEnd + if (qos > 0) 2 else 0
            return PublishedPacket(topic, body.copyOfRange(payloadOffset, body.size).toString(Charsets.UTF_8), qos,
                packet.header and 0x01 != 0)
        }

        private fun readPacket(input: DataInputStream): Packet? {
            val header = try { input.readUnsignedByte() } catch (_: java.io.EOFException) { return null }
            var remaining = 0
            var multiplier = 1
            while (true) {
                val digit = input.readUnsignedByte()
                remaining += (digit and 0x7f) * multiplier
                if (digit and 0x80 == 0) break
                multiplier *= 128
                check(multiplier <= 128 * 128 * 128) { "Malformed MQTT remaining-length field" }
            }
            check(remaining <= 1_048_576) { "Unexpectedly large loopback MQTT packet" }
            val body = ByteArray(remaining)
            input.readFully(body)
            return Packet(header, body)
        }

        private data class Packet(val header: Int, val body: ByteArray)
    }
}
