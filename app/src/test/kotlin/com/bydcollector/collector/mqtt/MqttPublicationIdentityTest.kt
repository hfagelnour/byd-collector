package com.bydcollector.collector.mqtt

import com.bydcollector.collector.ha.HaEndpointProfile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class MqttPublicationIdentityTest {
    @Test
    fun destinationIdentityExcludesSecretsAndClientIdButBindsBrokerAccountAndTopics() {
        val base = config()
        val key = MqttDestinationIdentity.destinationKey(base, NoopMqttPublicationJournalStore)
        assertEquals(
            key,
            MqttDestinationIdentity.destinationKey(base.copy(password = "different-secret", clientId = "different-client"), NoopMqttPublicationJournalStore)
        )
        assertNotEquals(key, MqttDestinationIdentity.destinationKey(base.copy(username = "another-user"), NoopMqttPublicationJournalStore))
        assertNotEquals(key, MqttDestinationIdentity.destinationKey(base.copy(host = "other.local"), NoopMqttPublicationJournalStore))
        assertNotEquals(key, MqttDestinationIdentity.destinationKey(base.copy(port = 1884), NoopMqttPublicationJournalStore))
        assertNotEquals(key, MqttDestinationIdentity.destinationKey(base.copy(deviceId = "another_vehicle"), NoopMqttPublicationJournalStore))
        assertNotEquals(key, MqttDestinationIdentity.destinationKey(base.copy(topicPrefix = "other"), NoopMqttPublicationJournalStore))
        assertNotEquals(key, MqttDestinationIdentity.destinationKey(base.copy(discoveryPrefix = "other_ha"), NoopMqttPublicationJournalStore))
        assertTrue(key.matches(Regex("[a-f0-9]{64}")))
        assertTrue("secret" !in key && "private-user" !in key)
    }

    @Test
    fun exactEndpointAliasesShareOneScopeAndEpochChangesDestinationGeneration() {
        val base = config().copy(alternativeHost = "mqtt.local", alternativePort = 1883)
        val scopes = MqttDestinationIdentity.configuredScopes(base)
        assertEquals(1, scopes.size)
        assertEquals(HaEndpointProfile.PRIMARY, scopes.single().profile)

        var epoch = 0L
        val journal = object : MqttPublicationJournalStore by NoopMqttPublicationJournalStore {
            override fun destinationEpoch(scopeKey: String, deviceId: String): Long = epoch
        }
        val before = MqttDestinationIdentity.destinationKey(base, journal)
        epoch += 1L
        assertNotEquals(before, MqttDestinationIdentity.destinationKey(base, journal))
    }

    private fun config(): HaMqttConfig = HaMqttConfig(
        enabled = true,
        discoveryEnabled = true,
        host = "mqtt.local",
        port = 1883,
        username = "private-user",
        password = "secret",
        clientId = "runtime-client",
        topicPrefix = "bydcollector",
        discoveryPrefix = "homeassistant",
        deviceId = "sealion_07",
        enabledCategories = HaMqttConfig.DEFAULT_CATEGORIES
    )
}
