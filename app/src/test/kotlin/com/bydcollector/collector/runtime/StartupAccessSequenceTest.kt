package com.bydcollector.collector.runtime

import com.bydcollector.collector.runtime.StartupAccessSequence.Step
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StartupAccessSequenceTest {
    @Test fun startupWaitsForEachTerminalResultIncludingAdbFailureAndPermissionDenial() {
        val flow = StartupAccessSequence()
        for (step in listOf(Step.BACKGROUND, Step.ADB, Step.OVERLAY, Step.LOCATION)) {
            assertEquals(step, flow.step)
            assertFalse(flow.begin(resumed = false, focused = true))
            assertFalse(flow.begin(resumed = true, focused = false))
            assertTrue(flow.begin(resumed = true, focused = true))
            assertFalse(flow.begin(resumed = true, focused = true))
            flow.complete(step)
        }
        assertEquals(Step.COMPLETE, flow.step)
        assertFalse(flow.begin(resumed = true, focused = true))
    }

    @Test fun staleCallbackCannotSkipTheCurrentPermission() {
        val flow = StartupAccessSequence()
        flow.complete(Step.BACKGROUND)
        flow.complete(Step.ADB)
        flow.begin(true, true)
        flow.complete(Step.ADB)
        assertEquals(Step.OVERLAY, flow.step)
        assertTrue(flow.inFlight)
    }

    @Test fun ManualRequestsDoNotRepeatUnrelatedPermissions() {
        for ((request, step) in listOf("adb" to Step.ADB, "overlay" to Step.OVERLAY, "background" to Step.BACKGROUND)) {
            val flow = StartupAccessSequence(request)
            assertEquals(step, flow.step)
            assertTrue(flow.begin(true, true))
            flow.complete(step)
            assertEquals(Step.COMPLETE, flow.step)
        }
    }
}
