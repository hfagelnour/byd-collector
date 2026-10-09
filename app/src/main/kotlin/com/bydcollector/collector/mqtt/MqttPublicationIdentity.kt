package com.bydcollector.collector.mqtt

import com.bydcollector.collector.ha.HaEndpointProfile
import java.security.MessageDigest
import java.util.UUID

data class MqttBrokerScope(
    val key: String,
    val host: String,
    val port: Int,
    val usernameFingerprint: String,
    val profile: HaEndpointProfile
)

data class MqttFrozenSession(
    val token: String,
    val deviceId: String,
    val scopeKeys: Set<String>
)

object MqttPublicationGate {
    private val monitor = Any()
    private val sessions = linkedMapOf<String, MqttFrozenSession>()

    fun <T> withLock(action: () -> T): T = synchronized(monitor, action)

    fun registerSession(deviceId: String, scopeKeys: Set<String>): String = withLock {
        UUID.randomUUID().toString().also { token ->
            sessions[token] = MqttFrozenSession(token, deviceId, scopeKeys.toSet())
        }
    }

    fun unregisterSession(token: String?) {
        if (token == null) return
        withLock { sessions.remove(token) }
    }

    fun activeSessions(): List<MqttFrozenSession> = withLock { sessions.values.toList() }
}

object MqttDestinationIdentity {
    fun configuredScopes(config: HaMqttConfig): List<MqttBrokerScope> = buildList {
        for (profile in HaEndpointProfile.entries) {
            scopeForProfile(config, profile)?.let(::add)
        }
    }.distinctBy { it.key }

    fun scopeForProfile(config: HaMqttConfig, profile: HaEndpointProfile): MqttBrokerScope? {
        val endpoint = runCatching { config.forProfile(profile) }.getOrNull() ?: return null
        val usernameFingerprint = sha256(listOf(endpoint.username.orEmpty()))
        val scopeKey = sha256(listOf(endpoint.host, endpoint.port.toString(), usernameFingerprint))
        return MqttBrokerScope(
            key = scopeKey,
            host = endpoint.host,
            port = endpoint.port,
            usernameFingerprint = usernameFingerprint,
            profile = profile
        )
    }

    fun destinationKey(config: HaMqttConfig, journal: MqttPublicationJournalStore): String {
        val deviceId = config.normalizedDeviceId()
        val fields = buildList {
            add(config.normalizedDeviceId())
            add(config.normalizedDiscoveryPrefix())
            add(config.normalizedTopicPrefix())
            configuredScopes(config).sortedBy { it.key }.forEach { scope ->
                add(scope.key)
                add(journal.destinationEpoch(scope.key, deviceId).toString())
            }
        }
        return sha256(fields)
    }

    private fun sha256(fields: List<String>): String {
        val bytes = fields.joinToString(separator = "") { "${it.length}:$it" }.toByteArray(Charsets.UTF_8)
        return MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    }
}
