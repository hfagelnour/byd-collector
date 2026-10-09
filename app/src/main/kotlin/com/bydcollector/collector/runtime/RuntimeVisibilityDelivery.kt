package com.bydcollector.collector.runtime

/** Coalesces lifecycle demand; a timed-out owner must eventually receive the latest state. */
internal class RuntimeVisibilityDelivery(
    private val schedule: (Runnable, Long) -> Unit,
    private val deliver: (Boolean, Boolean) -> Unit,
    private val canRetry: () -> Boolean,
    private val onFailure: (Throwable) -> Unit
) {
    private val lock = Any()
    private var desired = false to false
    private var revision = 0L
    private var pending = false
    private val attempt = Runnable { sendLatest() }

    fun update(ui: Boolean, kpi: Boolean) {
        val start = synchronized(lock) {
            desired = ui to kpi
            revision++
            if (pending) false else { pending = true; true }
        }
        if (start) schedule(attempt, 0L)
    }

    private fun sendLatest() {
        val (generation, state) = synchronized(lock) { revision to desired }
        val result = runCatching { deliver(state.first, state.second) }
        result.exceptionOrNull()?.let { error -> runCatching { onFailure(error) } }
        val retry = result.isFailure && runCatching(canRetry).getOrDefault(false)
        val delay = synchronized(lock) {
            when {
                generation != revision -> 0L
                retry -> 1_000L
                else -> { pending = false; null }
            }
        }
        if (delay != null) schedule(attempt, delay)
    }
}
