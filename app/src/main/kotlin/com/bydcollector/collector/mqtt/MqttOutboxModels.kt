package com.bydcollector.collector.mqtt

data class PendingMqttMessage(
    val targetKey: String,
    val targetType: String,
    val payloadHash: String = "",
    val message: HaMqttMessage,
    val priority: Int,
    val attemptCount: Int,
    val destinationKey: String? = null
)

interface MqttOutboxStore {
    fun upsertPending(message: HaMqttMessage, targetType: String, priority: Int, destinationKey: String? = null)
    fun dueMessages(nowIso: String, limit: Int): List<PendingMqttMessage>
    fun pendingMessages(limit: Int): List<PendingMqttMessage> {
        return dueMessages("9999-12-31T23:59:59Z", limit)
    }
    fun isCurrent(targetKey: String, payloadHash: String, destinationKey: String?): Boolean
    fun discardPending(targetKey: String, payloadHash: String, destinationKey: String?): Boolean
    fun markAttempt(targetKey: String, payloadHash: String, attemptedAt: String, destinationKey: String? = null): Boolean
    fun markFailed(
        targetKey: String,
        payloadHash: String,
        error: String,
        failedAt: String,
        nextAttemptAt: String?,
        destinationKey: String? = null
    ): Boolean
    fun markPublished(targetKey: String, payloadHash: String, publishedAt: String, destinationKey: String? = null): Boolean
    fun pendingCount(): Long
}
