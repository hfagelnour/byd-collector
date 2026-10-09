package com.bydcollector.collector.mqtt

import com.bydcollector.collector.data.local.Clock
import com.bydcollector.collector.data.local.SystemClockAdapter
import java.util.UUID

data class MqttCleanupExecution(
    val operationId: String?,
    val result: MqttActionResult
)

/** Executes exact, journal-backed retained discovery cleanup without a Will or broker failover. */
class MqttCleanupCore(
    private val journal: MqttPublicationJournalStore,
    private val clientFactory: () -> MqttClientFacade = { PahoMqttClientFacade() },
    private val configProvider: () -> HaMqttConfig,
    private val clock: Clock = SystemClockAdapter()
) {
    private sealed interface CleanupStart {
        data class Ready(
            val operationId: String,
            val completed: Boolean,
            val scopes: Map<String, MqttBrokerScope>
        ) : CleanupStart
        data class Completed(val operationId: String) : CleanupStart
        data class Rejected(val result: MqttActionResult) : CleanupStart
    }

    fun groups(): List<MqttCleanupGroup> {
        val config = configProvider()
        val scopes = MqttDestinationIdentity.configuredScopes(config).map { it.key }.toSet()
        val currentId = config.normalizedDeviceId()
        val recorded = journal.cleanupGroups(scopes, currentId)
        val current = recorded.firstOrNull { it.current }?.copy(
            deviceName = config.deviceName, deviceIdentifier = config.deviceIdentifier, deviceModel = config.deviceModel
        ) ?: MqttCleanupGroup.current(config, "no_history")
        // Configuration is visible even before a publication; it is not invented journal evidence.
        return listOf(current) + recorded.filterNot { it.current }
    }

    fun execute(
        selection: MqttCleanupSelection,
        isCancelled: () -> Boolean = { Thread.currentThread().isInterrupted }
    ): MqttCleanupExecution {
        var operationId: String? = null
        var failedTarget: MqttCleanupTargetRef? = null
        val clients = linkedMapOf<String, MqttClientFacade>()

        try {
            val started = MqttPublicationGate.withLock {
                journal.completedCleanupOperation(selection)?.let { completedId ->
                    return@withLock CleanupStart.Completed(completedId)
                }
                val config = configProvider()
                val scopes = MqttDestinationIdentity.configuredScopes(config).associateBy { it.key }
                val scopeKeys = scopes.keys
                config.validateEndpoints()?.let { message ->
                    return@withLock CleanupStart.Rejected(MqttActionResult.fail("mqtt_endpoint_invalid", message, MqttFailureKind.PROTOCOL))
                }
                config.validateIdentity()?.let { message ->
                    return@withLock CleanupStart.Rejected(MqttActionResult.fail("mqtt_identity_invalid", message, MqttFailureKind.PROTOCOL))
                }
                if (selection.deviceId == config.normalizedDeviceId()) {
                    return@withLock CleanupStart.Rejected(MqttActionResult.fail("mqtt_cleanup_current_id", "The current MQTT Device ID cannot be cleaned up", MqttFailureKind.PROTOCOL))
                }
                val targets = journal.cleanupSelectionTargets(selection, scopeKeys)
                if (targets.isEmpty()) {
                    return@withLock CleanupStart.Rejected(MqttActionResult.fail("mqtt_cleanup_stale", "MQTT cleanup selection is stale; refresh the published-ID list", MqttFailureKind.PROTOCOL))
                }
                if (MqttPublicationGate.activeSessions().any { it.deviceId == selection.deviceId }) {
                    return@withLock CleanupStart.Rejected(MqttActionResult.fail("mqtt_cleanup_active_id", "This MQTT Device ID is currently frozen by an active export session", MqttFailureKind.PROTOCOL))
                }
                val id = journal.beginCleanup(selection, scopeKeys, clock.nowIso())
                CleanupStart.Ready(id, journal.cleanupOperationCompleted(id), scopes)
            }
            when (started) {
                is CleanupStart.Rejected -> return MqttCleanupExecution(null, started.result)
                is CleanupStart.Completed -> return MqttCleanupExecution(started.operationId, MqttActionResult.ok("MQTT cleanup already completed"))
                is CleanupStart.Ready -> {
                    operationId = started.operationId
                    if (started.completed) {
                        return MqttCleanupExecution(started.operationId, MqttActionResult.ok("MQTT cleanup already completed"))
                    }
                }
            }

            val ready = started as CleanupStart.Ready
            val operation = ready.operationId
            val scopes = ready.scopes
            val currentTargets = journal.cleanupTargets(operation)
            if (currentTargets.isEmpty()) {
                val error = "MQTT cleanup has no recorded targets"
                journal.failCleanup(operation, null, error, clock.nowIso())
                return MqttCleanupExecution(operation, MqttActionResult.fail("mqtt_cleanup_stale", error, MqttFailureKind.PROTOCOL))
            }

            for (item in currentTargets) {
                failedTarget = item.target
                if (isCancelled()) {
                    val result = MqttActionResult.fail("mqtt_cleanup_cancelled", "MQTT cleanup was cancelled", MqttFailureKind.CANCELLED)
                    journal.failCleanup(operation, item.target, result.message, clock.nowIso())
                    return MqttCleanupExecution(operation, result)
                }
                if (item.acknowledged) continue

                val selectedScope = scopes[item.target.scopeKey]
                if (selectedScope == null) {
                    val result = MqttActionResult.fail("mqtt_cleanup_scope_missing", "A recorded MQTT broker is no longer configured", MqttFailureKind.PROTOCOL)
                    journal.failCleanup(operation, item.target, result.message, clock.nowIso())
                    return MqttCleanupExecution(operation, result)
                }
                var client = clients[item.target.scopeKey]
                if (client == null) {
                    val latestConfig = configProvider()
                    if (latestConfig.validateEndpoints() != null || latestConfig.validateIdentity() != null ||
                        latestConfig.normalizedDeviceId() == selection.deviceId) {
                        val result = MqttActionResult.fail("mqtt_cleanup_config_changed", "MQTT settings changed or now select the Device ID being cleaned", MqttFailureKind.PROTOCOL)
                        journal.failCleanup(operation, item.target, result.message, clock.nowIso())
                        return MqttCleanupExecution(operation, result)
                    }
                    val latestScope = MqttDestinationIdentity.configuredScopes(latestConfig)
                        .firstOrNull { it.key == selectedScope.key }
                    if (latestScope == null) {
                        val result = MqttActionResult.fail("mqtt_cleanup_scope_missing", "A recorded MQTT broker is no longer configured", MqttFailureKind.PROTOCOL)
                        journal.failCleanup(operation, item.target, result.message, clock.nowIso())
                        return MqttCleanupExecution(operation, result)
                    }
                    val base = runCatching { latestConfig.forProfile(latestScope.profile) }.getOrElse {
                        val result = MqttActionResult.fail("mqtt_cleanup_scope_missing", "A recorded MQTT broker is no longer configured", MqttFailureKind.PROTOCOL)
                        journal.failCleanup(operation, item.target, result.message, clock.nowIso())
                        return MqttCleanupExecution(operation, result)
                    }
                    val newClient = clientFactory()
                    client = newClient
                    clients[item.target.scopeKey] = newClient
                    val connect = newClient.connect(
                        base.copy(clientId = cleanupClientId()),
                        willMessage = null,
                        purpose = MqttConnectionPurpose.TEST_NO_WILL
                    )
                    if (!connect.ok) {
                        journal.failCleanup(operation, item.target, connect.message, clock.nowIso())
                        return MqttCleanupExecution(operation, connect)
                    }
                }
                val activeClient = checkNotNull(client)

                val publishResult = MqttPublicationGate.withLock {
                    val latestConfig = configProvider()
                    val stillConfigured = MqttDestinationIdentity.configuredScopes(latestConfig)
                        .any { it.key == item.target.scopeKey }
                    when {
                        isCancelled() -> MqttActionResult.fail("mqtt_cleanup_cancelled", "MQTT cleanup was cancelled", MqttFailureKind.CANCELLED)
                        latestConfig.validateEndpoints() != null || latestConfig.validateIdentity() != null -> MqttActionResult.fail("mqtt_cleanup_config_changed", "MQTT settings are no longer valid for this cleanup", MqttFailureKind.PROTOCOL)
                        latestConfig.normalizedDeviceId() == selection.deviceId -> MqttActionResult.fail("mqtt_cleanup_current_id", "The current MQTT Device ID cannot be cleaned up", MqttFailureKind.PROTOCOL)
                        MqttPublicationGate.activeSessions().any { it.deviceId == selection.deviceId } -> MqttActionResult.fail("mqtt_cleanup_active_id", "This MQTT Device ID is currently frozen by an active export session", MqttFailureKind.PROTOCOL)
                        !stillConfigured -> MqttActionResult.fail("mqtt_cleanup_scope_missing", "A recorded MQTT broker is no longer configured", MqttFailureKind.PROTOCOL)
                        else -> {
                            val publish = activeClient.publish(
                                HaMqttMessage(item.target.topic, "", retained = true, qos = 1)
                            )
                            if (publish.ok) journal.acknowledgeCleanupTarget(operation, item.target, clock.nowIso())
                            publish
                        }
                    }
                }
                if (!publishResult.ok) {
                    journal.failCleanup(operation, item.target, publishResult.message, clock.nowIso())
                    return MqttCleanupExecution(operation, publishResult)
                }
            }

            journal.completeCleanup(operation, clock.nowIso())
            return MqttCleanupExecution(operation, MqttActionResult.ok("MQTT discovery cleanup completed"))
        } catch (error: Exception) {
            val result = MqttActionResult.fail(
                "mqtt_cleanup_error",
                "${error::class.java.simpleName}: ${error.message ?: "MQTT cleanup failed"}",
                MqttFailureKind.OTHER
            )
            operationId?.let { id -> runCatching { journal.failCleanup(id, failedTarget, result.message, clock.nowIso()) } }
            return MqttCleanupExecution(operationId, result)
        } finally {
            clients.values.forEach { client -> runCatching { client.disconnect(null) } }
        }
    }

    private fun cleanupClientId(): String = "bydc_${UUID.randomUUID().toString().replace("-", "")}"
}
