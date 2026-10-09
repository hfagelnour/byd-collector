package com.bydcollector.collector.mqtt

import com.bydcollector.collector.data.local.Clock
import com.bydcollector.collector.ha.HaEndpointProfile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class MqttCleanupCoreTest {
    @Test
    fun currentIdWithoutHistoryIsVisibleProtectedAndDoesNotInventJournalEntries() {
        val config = config()
        val scope = MqttDestinationIdentity.configuredScopes(config).single().key
        val journal = InMemoryMqttPublicationJournal()
        val client = RecordingCleanupClient()
        val core = MqttCleanupCore(journal, { client }, { config }, FixedClock())
        val current = core.groups().single()
        assertEquals(config.deviceId, current.deviceId)
        assertEquals(config.deviceIdentifier, current.deviceIdentifier)
        assertEquals(config.deviceModel, current.deviceModel)
        assertEquals("${config.deviceId} | ${config.deviceIdentifier} | ${config.deviceName} | ${config.deviceModel}", current.identityLine())
        assertTrue(current.current)
        assertEquals("no_history", current.state)
        assertTrue(journal.cleanupGroups(setOf(scope), config.deviceId).isEmpty())
        assertEquals("mqtt_cleanup_current_id", core.execute(current.selection).result.category)
        assertEquals(0, client.connectCalls)

        journal.recordDiscoveryAcknowledged(scope, config.deviceId,
            "ha/sensor/${config.deviceId}/speed/config", "hash", "Car", "now", false)
        val recorded = core.groups().single()
        assertTrue(recorded.current)
        assertEquals(1, recorded.topicCount)
        assertNotEquals("no_history", recorded.state)

        val otherBroker = MqttCleanupCore(journal, { client }, { config.copy(host = "other.local") }, FixedClock())
        assertEquals("no_history", otherBroker.groups().single().state)
    }

    @Test
    fun historicalIdentityKeepsPublishedFieldsAndNeverBorrowsCurrentMetadata() {
        val config = config()
        val scope = MqttDestinationIdentity.configuredScopes(config).single().key
        val journal = InMemoryMqttPublicationJournal()
        journal.recordDiscoveryAcknowledged(scope, "old", "ha/old/config", "hash", "Old car", "now", false,
            deviceIdentifier = "old_identifier", deviceModel = "Old model")
        journal.recordDiscoveryAttempt(scope, "legacy", "ha/legacy/config", "hash", "Legacy car", "now")
        val groups = MqttCleanupCore(journal, configProvider = { config }).groups()
        assertEquals("old | old_identifier | Old car | Old model", groups.single { it.deviceId == "old" }.identityLine())
        assertEquals("legacy | — | Legacy car | —", groups.single { it.deviceId == "legacy" }.identityLine())
        val immediate = MqttCleanupGroup.current(config)
        assertTrue(immediate.current)
        assertEquals("loading", immediate.state)
        assertEquals("", immediate.selection.fingerprint)
        assertEquals(groups.first().identityLine(), immediate.identityLine())
    }

    @Test
    fun partialCleanupRetriesOnlyUnacknowledgedTopicsAndReceiptCannotTouchNewGeneration() {
        val config = config()
        val scope = MqttDestinationIdentity.configuredScopes(config).single().key
        val journal = InMemoryMqttPublicationJournal()
        val topics = listOf("ha/sensor/retired_vehicle/battery/config", "ha/sensor/retired_vehicle/range/config")
        topics.forEachIndexed { index, topic ->
            journal.recordDiscoveryAcknowledged(scope, "retired_vehicle", topic, "hash_$index", "Retired vehicle", "t$index", remove = false)
        }
        val selection = journal.cleanupGroups(setOf(scope), config.deviceId).single().selection
        val client = RecordingCleanupClient().apply {
            publishResults += MqttActionResult.ok()
            publishResults += MqttActionResult.fail("mqtt_error", "broker dropped", MqttFailureKind.TRANSPORT)
        }
        val core = MqttCleanupCore(journal, { client }, { config }, FixedClock())

        val first = core.execute(selection)
        assertFalse(first.result.ok)
        assertEquals(2, client.published.size)
        assertEquals(2, journal.cleanupTargets(checkNotNull(first.operationId)).size)
        assertEquals(1, journal.cleanupTargets(checkNotNull(first.operationId)).count { it.acknowledged })
        assertEquals("error", core.groups().single { !it.current }.state)

        client.publishResults += MqttActionResult.ok()
        val retry = core.execute(selection)
        assertTrue(retry.result.ok)
        assertEquals(topics, client.published.map { it.topic }.distinct())
        assertEquals(1, client.published.count { it.topic == topics[0] })
        assertEquals(2, client.published.count { it.topic == topics[1] })
        assertTrue(client.wills.all { it == null })
        assertTrue(client.purposes.all { it == MqttConnectionPurpose.TEST_NO_WILL })
        assertTrue(client.configs.all { it.clientId.startsWith("bydc_") && it.clientId != config.clientId })
        assertNotEquals(client.configs[0].clientId, client.configs[1].clientId)
        assertTrue(core.groups().single().current)

        val priorOperation = checkNotNull(retry.operationId)
        assertTrue(journal.cleanupOperationCompleted(priorOperation))
        journal.recordDiscoveryAcknowledged(scope, "retired_vehicle", topics[0], "new-hash", "Retired vehicle", "later", remove = false)
        val laterSelection = journal.cleanupGroups(setOf(scope), config.deviceId).single().selection
        assertNotEquals(selection, laterSelection, "A later publication generation must invalidate the old selection")
        val publishedBeforeReplay = client.published.size
        val replay = core.execute(selection)
        assertTrue(replay.result.ok)
        assertEquals(publishedBeforeReplay, client.published.size, "Completed-job replay must not touch a newer publication")
        assertEquals(1, core.groups().single { !it.current }.topicCount)
    }

    @Test
    fun currentAndFrozenDeviceIdsAreProtectedAndCancellationPersistsPartialProgress() {
        val currentConfig = config(deviceId = "retired_vehicle")
        val scope = MqttDestinationIdentity.configuredScopes(currentConfig).single().key
        val journal = InMemoryMqttPublicationJournal()
        val target = "retired_vehicle"
        listOf("ha/sensor/retired_vehicle/a/config", "ha/sensor/retired_vehicle/b/config").forEach {
            journal.recordDiscoveryAcknowledged(scope, target, it, "hash", "Vehicle", "now", remove = false)
        }
        val currentSelection = journal.cleanupGroups(setOf(scope), currentConfig.deviceId).single()
        assertTrue(currentSelection.current)
        val unusedClient = RecordingCleanupClient()
        val currentResult = MqttCleanupCore(journal, { unusedClient }, { currentConfig }, FixedClock()).execute(currentSelection.selection)
        assertEquals("mqtt_cleanup_current_id", currentResult.result.category)
        assertEquals(0, unusedClient.connectCalls)

        val safeConfig = currentConfig.copy(deviceId = "active_vehicle")
        val selection = journal.cleanupGroups(setOf(scope), safeConfig.deviceId).single().selection
        val frozenToken = MqttPublicationGate.registerSession(target, setOf(scope))
        try {
            val frozenClient = RecordingCleanupClient()
            val frozenResult = MqttCleanupCore(journal, { frozenClient }, { safeConfig }, FixedClock()).execute(selection)
            assertEquals("mqtt_cleanup_active_id", frozenResult.result.category)
            assertEquals(0, frozenClient.connectCalls)
        } finally {
            MqttPublicationGate.unregisterSession(frozenToken)
        }

        val client = RecordingCleanupClient()
        val core = MqttCleanupCore(journal, { client }, { safeConfig }, FixedClock())
        val cancelled = core.execute(selection) { client.published.isNotEmpty() }
        assertEquals("mqtt_cleanup_cancelled", cancelled.result.category)
        assertEquals(1, client.published.size)
        assertEquals(1, journal.cleanupTargets(checkNotNull(cancelled.operationId)).count { it.acknowledged })
        assertTrue(core.groups().single { !it.current }.error?.contains("cancelled") == true)
    }

    @Test
    fun cleanupOnlyTouchesScopesStillConfiguredAndDoesNotFailOver() {
        val currentConfig = config(alternativeHost = "backup.local")
        val scopes = MqttDestinationIdentity.configuredScopes(currentConfig).associateBy { it.profile }
        val primary = checkNotNull(scopes[HaEndpointProfile.PRIMARY])
        val alternative = checkNotNull(scopes[HaEndpointProfile.ALTERNATIVE])
        val journal = InMemoryMqttPublicationJournal()
        val primaryTopic = "ha/sensor/retired_vehicle/primary/config"
        val altTopic = primaryTopic
        journal.recordDiscoveryAcknowledged(primary.key, "retired_vehicle", primaryTopic, "p", "Car", "now", false)
        journal.recordDiscoveryAcknowledged(alternative.key, "retired_vehicle", altTopic, "a", "Car", "now", false)
        assertEquals(1, journal.cleanupGroups(scopes.values.map { it.key }.toSet(), currentConfig.deviceId).size)
        assertEquals(1, journal.cleanupGroups(scopes.values.map { it.key }.toSet(), currentConfig.deviceId).single().topicCount)

        val reducedConfig = currentConfig.copy(alternativeHost = null, alternativePort = null)
        val visible = MqttDestinationIdentity.configuredScopes(reducedConfig).map { it.key }.toSet()
        val group = journal.cleanupGroups(visible, reducedConfig.deviceId).single()
        assertEquals(1, group.topicCount)
        val client = RecordingCleanupClient()
        val result = MqttCleanupCore(journal, { client }, { reducedConfig }, FixedClock()).execute(group.selection)
        assertTrue(result.result.ok)
        assertEquals(listOf(primaryTopic), client.published.map { it.topic })
        assertEquals("mqtt.local", client.configs.single().host)
        assertEquals(1, journal.cleanupGroups(setOf(alternative.key), reducedConfig.deviceId).single().topicCount)
    }

    private class RecordingCleanupClient : MqttClientFacade {
        override var isConnected: Boolean = false
            private set
        val configs = mutableListOf<HaMqttConfig>()
        val purposes = mutableListOf<MqttConnectionPurpose>()
        val wills = mutableListOf<HaMqttMessage?>()
        val published = mutableListOf<HaMqttMessage>()
        val publishResults = mutableListOf<MqttActionResult>()
        var connectCalls = 0

        override fun connect(config: HaMqttConfig, willMessage: HaMqttMessage?): MqttActionResult =
            connect(config, willMessage, MqttConnectionPurpose.RUNTIME)

        override fun connect(config: HaMqttConfig, willMessage: HaMqttMessage?, purpose: MqttConnectionPurpose): MqttActionResult {
            connectCalls += 1
            configs += config
            purposes += purpose
            wills += willMessage
            isConnected = true
            return MqttActionResult.ok()
        }

        override fun publish(message: HaMqttMessage): MqttActionResult {
            check(isConnected)
            published += message
            return if (publishResults.isEmpty()) MqttActionResult.ok() else publishResults.removeAt(0)
        }

        override fun disconnect(gracefulMessage: HaMqttMessage?): MqttActionResult {
            isConnected = false
            return MqttActionResult.ok()
        }
    }

    private class FixedClock : Clock {
        private var tick = 0
        override fun nowIso(): String = "2026-10-09T00:00:${(tick++).toString().padStart(2, '0')}Z"
        override fun elapsedRealtimeMs(): Long = 0
    }

    private fun config(deviceId: String = "current_vehicle", alternativeHost: String? = null): HaMqttConfig = HaMqttConfig(
        enabled = false,
        discoveryEnabled = true,
        host = "mqtt.local",
        port = 1883,
        username = "user",
        password = "secret",
        clientId = "original-client",
        topicPrefix = "bydcollector",
        discoveryPrefix = "ha",
        deviceId = deviceId,
        enabledCategories = HaMqttConfig.DEFAULT_CATEGORIES,
        alternativeHost = alternativeHost,
        alternativePort = alternativeHost?.let { 1884 }
    )
}
