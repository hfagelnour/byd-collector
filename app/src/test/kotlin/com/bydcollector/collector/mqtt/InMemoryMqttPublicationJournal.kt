package com.bydcollector.collector.mqtt

import java.security.MessageDigest
import java.util.UUID

internal data class MqttJournalPublishRecord(
    val scopeKey: String,
    val deviceId: String,
    val topic: String,
    val payloadHash: String,
    val remove: Boolean = false
)

internal class InMemoryMqttPublicationJournal : MqttPublicationJournalStore {
    private data class History(
        var acknowledgedHash: String? = null,
        var acknowledgedName: String? = null,
        var acknowledgedIdentifier: String? = null,
        var acknowledgedModel: String? = null,
        var uncertainHash: String? = null,
        var uncertainName: String? = null,
        var uncertainIdentifier: String? = null,
        var uncertainModel: String? = null,
        var generation: Long = 0L,
        var error: String? = null
    )

    private data class Operation(
        val id: String,
        val deviceId: String,
        val fingerprint: String,
        val targets: MutableMap<MqttCleanupTargetRef, Pair<Boolean, String?>>,
        var state: String = "running",
        var error: String? = null
    )

    private val history = linkedMapOf<MqttCleanupTargetRef, History>()
    private val epochs = mutableMapOf<Pair<String, String>, Long>()
    private val operations = linkedMapOf<String, Operation>()
    val attempts = mutableListOf<MqttJournalPublishRecord>()
    val acknowledgements = mutableListOf<MqttJournalPublishRecord>()

    fun trackedTopicCount(): Int = history.size
    fun isTracked(scopeKey: String, deviceId: String, topic: String): Boolean = MqttCleanupTargetRef(scopeKey, deviceId, topic) in history

    override fun destinationEpoch(scopeKey: String, deviceId: String): Long = epochs[scopeKey to deviceId] ?: 0L

    override fun isCleanupPending(deviceId: String, configuredScopeKeys: Set<String>): Boolean = operations.values.any { operation ->
        operation.deviceId == deviceId && operation.state in setOf("running", "error") &&
            operation.targets.keys.any { it.scopeKey in configuredScopeKeys }
    }

    override fun recordDiscoveryAttempt(scopeKey: String, deviceId: String, topic: String, payloadHash: String, deviceName: String, at: String, deviceIdentifier: String?, deviceModel: String?) {
        attempts += MqttJournalPublishRecord(scopeKey, deviceId, topic, payloadHash)
        val target = MqttCleanupTargetRef(scopeKey, deviceId, topic)
        val row = history.getOrPut(target) { History() }
        row.uncertainHash = payloadHash
        row.uncertainName = deviceName
        row.uncertainIdentifier = deviceIdentifier
        row.uncertainModel = deviceModel
        row.generation = destinationEpoch(scopeKey, deviceId)
        row.error = null
    }

    override fun recordDiscoveryAcknowledged(scopeKey: String, deviceId: String, topic: String, payloadHash: String, deviceName: String, at: String, remove: Boolean, deviceIdentifier: String?, deviceModel: String?) {
        acknowledgements += MqttJournalPublishRecord(scopeKey, deviceId, topic, payloadHash, remove)
        val target = MqttCleanupTargetRef(scopeKey, deviceId, topic)
        if (remove) history.remove(target) else {
            val row = history.getOrPut(target) { History() }
            row.acknowledgedHash = payloadHash
            row.acknowledgedName = deviceName
            row.acknowledgedIdentifier = deviceIdentifier
            row.acknowledgedModel = deviceModel
            row.uncertainHash = null
            row.uncertainName = null
            row.uncertainIdentifier = null
            row.uncertainModel = null
            row.generation = destinationEpoch(scopeKey, deviceId)
            row.error = null
        }
    }

    override fun recordDiscoveryFailure(scopeKey: String, deviceId: String, topic: String, error: String, at: String) {
        history[MqttCleanupTargetRef(scopeKey, deviceId, topic)]?.error = error
    }

    override fun cleanupGroups(configuredScopeKeys: Set<String>, currentDeviceId: String): List<MqttCleanupGroup> =
        history.filterKeys { it.scopeKey in configuredScopeKeys }.entries.groupBy { it.key.deviceId }.map { (deviceId, rows) ->
            val targets = rows.map { it.key }
            val fingerprint = fingerprint(rows)
            val identity = rows.lastOrNull { it.value.uncertainHash != null }?.value ?: rows.lastOrNull()?.value
            val operation = operations.values.firstOrNull { it.deviceId == deviceId && it.state in setOf("running", "error") && it.targets.keys.any { target -> target.scopeKey in configuredScopeKeys } }
            MqttCleanupGroup(
                deviceId = deviceId,
                deviceName = rows.lastOrNull { it.value.uncertainHash != null }?.value?.uncertainName
                    ?: rows.lastOrNull()?.value?.acknowledgedName.orEmpty(),
                deviceIdentifier = if (identity?.uncertainHash != null) identity.uncertainIdentifier else identity?.acknowledgedIdentifier,
                deviceModel = if (identity?.uncertainHash != null) identity.uncertainModel else identity?.acknowledgedModel,
                topicCount = targets.map { it.topic }.distinct().size,
                uncertainCount = rows.filter { it.value.uncertainHash != null }.map { it.key.topic }.distinct().size,
                error = operation?.error,
                current = deviceId == currentDeviceId,
                state = when {
                    deviceId == currentDeviceId -> "current"
                    operation?.state == "error" -> "error"
                    operation?.state == "running" -> "cleaning"
                    rows.any { it.value.uncertainHash != null } -> "uncertain"
                    else -> "published"
                },
                selection = MqttCleanupSelection(deviceId, fingerprint)
            )
        }.sortedBy { it.deviceId }

    override fun cleanupSelectionTargets(selection: MqttCleanupSelection, configuredScopeKeys: Set<String>): List<MqttCleanupTargetRef> {
        val rows = history.filterKeys { it.deviceId == selection.deviceId && it.scopeKey in configuredScopeKeys }.entries
        if (rows.isEmpty() || fingerprint(rows) != selection.fingerprint) return emptyList()
        return rows.map { it.key }
    }

    override fun completedCleanupOperation(selection: MqttCleanupSelection): String? = operations.values.firstOrNull {
        it.deviceId == selection.deviceId && it.fingerprint == selection.fingerprint && it.state == "complete"
    }?.id

    override fun beginCleanup(selection: MqttCleanupSelection, configuredScopeKeys: Set<String>, startedAt: String): String {
        completedCleanupOperation(selection)?.let { return it }
        val targets = cleanupSelectionTargets(selection, configuredScopeKeys)
        check(targets.isNotEmpty()) { "stale selection" }
        operations.values.firstOrNull {
            it.deviceId == selection.deviceId && it.state in setOf("running", "error") && it.targets.keys.any { target -> target.scopeKey in targets.map { selected -> selected.scopeKey }.toSet() }
        }?.let { existing ->
            check(existing.fingerprint == selection.fingerprint) { "conflicting operation" }
            existing.state = "running"
            existing.error = null
            return existing.id
        }
        val id = UUID.randomUUID().toString()
        operations[id] = Operation(
            id = id,
            deviceId = selection.deviceId,
            fingerprint = selection.fingerprint,
            targets = targets.associateWith { false to null }.toMutableMap()
        )
        targets.map { it.scopeKey }.toSet().forEach { scope -> epochs[scope to selection.deviceId] = destinationEpoch(scope, selection.deviceId) + 1L }
        return id
    }

    override fun cleanupTargets(operationId: String): List<MqttCleanupOperationTarget> = operations[operationId]?.targets.orEmpty().map { (target, state) ->
        MqttCleanupOperationTarget(target, state.first)
    }

    override fun cleanupOperationCompleted(operationId: String): Boolean = operations[operationId]?.state == "complete"

    override fun acknowledgeCleanupTarget(operationId: String, target: MqttCleanupTargetRef, acknowledgedAt: String) {
        val operation = checkNotNull(operations[operationId])
        check(target in operation.targets)
        operation.targets[target] = true to null
        operation.state = "running"
        operation.error = null
    }

    override fun failCleanup(operationId: String, target: MqttCleanupTargetRef?, error: String, failedAt: String) {
        val operation = checkNotNull(operations[operationId])
        target?.let { current -> operation.targets[current]?.let { operation.targets[current] = it.first to error } }
        operation.state = "error"
        operation.error = error
    }

    override fun completeCleanup(operationId: String, completedAt: String) {
        val operation = checkNotNull(operations[operationId])
        check(operation.targets.isNotEmpty() && operation.targets.values.all { it.first })
        operation.targets.keys.forEach { history.remove(it) }
        operation.targets.clear()
        operation.state = "complete"
        operation.error = null
    }

    private fun fingerprint(rows: Collection<Map.Entry<MqttCleanupTargetRef, History>>): String {
        val fields = rows.sortedWith(compareBy({ it.key.scopeKey }, { it.key.deviceId }, { it.key.topic }))
            .flatMap { listOf(it.key.scopeKey, it.key.deviceId, it.key.topic, it.value.generation.toString()) }
        val bytes = fields.joinToString("") { "${it.length}:$it" }.toByteArray(Charsets.UTF_8)
        return MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    }
}
