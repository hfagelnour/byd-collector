package com.bydcollector.collector.runtime

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RuntimeVisibilityDeliveryTest {
    @Test
    fun timeoutRetriesLatestVisibilityAndShutdownStopsRetries() {
        val tasks = ArrayDeque<Pair<Runnable, Long>>()
        val delivered = mutableListOf<Pair<Boolean, Boolean>>()
        var blocked = true
        var shutdown = false
        var errors = 0
        val delivery = RuntimeVisibilityDelivery(
            schedule = { task, delay -> tasks.add(task to delay) },
            deliver = { ui, kpi -> if (blocked) error("owner timeout") else delivered.add(ui to kpi) },
            canRetry = { !shutdown }, onFailure = { errors++ }
        )
        delivery.update(true, true)
        tasks.removeFirst().first.run()
        assertEquals(1_000L, tasks.single().second)
        delivery.update(true, false)
        delivery.update(false, false)
        assertEquals(1, tasks.size)
        blocked = false
        tasks.removeFirst().first.run()
        assertEquals(listOf(false to false), delivered)
        assertEquals(1, errors)
        assertTrue(tasks.isEmpty())

        blocked = true
        shutdown = true
        delivery.update(false, false)
        tasks.removeFirst().first.run()
        assertTrue(tasks.isEmpty())
    }
}
