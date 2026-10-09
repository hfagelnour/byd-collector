package com.bydcollector.collector.runtime

/** One owner-package sequence; optional access advances on denial as well as success. */
internal class StartupAccessSequence(val request: String = "startup") {
    enum class Step { BACKGROUND, ADB, OVERLAY, LOCATION, COMPLETE }

    var step = when (request) {
        "startup", "background" -> Step.BACKGROUND
        "adb" -> Step.ADB
        "overlay" -> Step.OVERLAY
        else -> error("Unknown access request: $request")
    }
        private set
    var inFlight = false
        private set

    fun begin(resumed: Boolean, focused: Boolean): Boolean {
        if (!resumed || !focused || inFlight || step == Step.COMPLETE) return false
        inFlight = true
        return true
    }

    fun complete(expected: Step) {
        if (step != expected) return
        inFlight = false
        step = if (request != "startup") Step.COMPLETE else when (step) {
            Step.BACKGROUND -> Step.ADB
            Step.ADB -> Step.OVERLAY
            Step.OVERLAY -> Step.LOCATION
            Step.LOCATION, Step.COMPLETE -> Step.COMPLETE
        }
    }
}
