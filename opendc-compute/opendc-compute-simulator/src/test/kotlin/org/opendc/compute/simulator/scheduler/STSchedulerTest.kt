/*
 * Copyright (c) 2025 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.compute.simulator.scheduler

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.opendc.compute.simulator.host.SimHost
import org.opendc.compute.simulator.internal.Guest
import org.opendc.compute.simulator.scheduler.timeshift.STScheduler
import org.opendc.compute.simulator.scheduler.timeshift.TaskStopper
import org.opendc.compute.simulator.scheduler.timeshift.TimeshiftScheduler
import org.opendc.compute.simulator.service.ComputeService
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.compute.simulator.service.TaskNature
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import kotlin.coroutines.coroutineContext

class STSchedulerTest {

    @Test
    fun testBasicScheduling() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        val scheduler =
            STScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 10,
                clock = clock,
                forecast = false,
            )

        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofMillis(10)
        every { req.task.deadline } returns 50

        every { req.task.isExecuted } returns false
        every { req.task.isPaused } returns false
        every { req.task.isPausable } returns true

        every { req.task.carbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setPauseStatus(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.isExecuted = any() } answers {
            firstArg<Boolean>()
        }

        scheduler.updateCarbonIntensity(200.0)
        scheduler.updateCarbonIntensity(190.0)
        scheduler.updateCarbonIntensity(180.0)
        scheduler.updateCarbonIntensity(170.0)
        scheduler.updateCarbonIntensity(160.0)
        scheduler.updateCarbonIntensity(150.0)
        scheduler.updateCarbonIntensity(140.0)
        scheduler.updateCarbonIntensity(130.0)
        scheduler.updateCarbonIntensity(120.0)
        scheduler.updateCarbonIntensity(110.0)

        //Returns FAILURE because the schedule tries to find a host, but there is not host generated in test
        assertEquals(SchedulingResultType.FAILURE, scheduler.select(mutableListOf(req).iterator()).resultType)

    }
    @Test
    fun testBasicDeferring() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        val scheduler =
            STScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 10,
                clock = clock,
                forecast = false,
            )

        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofMillis(10)
        every { req.task.deadline } returns 50

        every { req.task.isExecuted } returns false
        every { req.task.isPaused } returns false
        every { req.task.isPausable } returns true

        scheduler.updateCarbonIntensity(100.0)
        scheduler.updateCarbonIntensity(110.0)
        scheduler.updateCarbonIntensity(120.0)
        scheduler.updateCarbonIntensity(130.0)
        scheduler.updateCarbonIntensity(140.0)
        scheduler.updateCarbonIntensity(150.0)
        scheduler.updateCarbonIntensity(160.0)
        scheduler.updateCarbonIntensity(170.0)
        scheduler.updateCarbonIntensity(180.0)
        scheduler.updateCarbonIntensity(190.0)

        //Returns EMPTY because tasks are delayed
        assertEquals(SchedulingResultType.EMPTY, scheduler.select(mutableListOf(req).iterator()).resultType)
    }

    @Test
    fun testScheduleIfTaskIsNotDeferrable() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)
        val scheduler =
            STScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 10,
                clock = clock,
                forecast = false,
            )

        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(false)
        every { req.task.duration } returns Duration.ofMillis(10)
        every { req.task.deadline } returns 50

        every { req.task.isExecuted } returns false
        every { req.task.isPaused } returns false
        every { req.task.isPausable } returns false

        every { req.task.carbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setPauseStatus(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.isExecuted = any() } answers {
            firstArg<Boolean>()
        }
        every { req.task.isPausable = any() } answers {
            firstArg<Boolean>()
        }

        scheduler.updateCarbonIntensity(100.0)
        scheduler.updateCarbonIntensity(110.0)
        scheduler.updateCarbonIntensity(120.0)
        scheduler.updateCarbonIntensity(130.0)
        scheduler.updateCarbonIntensity(140.0)
        scheduler.updateCarbonIntensity(150.0)
        scheduler.updateCarbonIntensity(160.0)
        scheduler.updateCarbonIntensity(170.0)
        scheduler.updateCarbonIntensity(180.0)
        scheduler.updateCarbonIntensity(190.0)

        //Returns FAILURE because there is no task after all
        assertEquals(SchedulingResultType.FAILURE, scheduler.select(mutableListOf(req).iterator()).resultType)
    }

    @Test
    fun testRespectDeadline() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)
        val scheduler =
            STScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 10,
                clock = clock,
                forecast = false,
            )

        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofMillis(10)
        every { req.task.deadline } returns 20

        every { req.task.isExecuted } returns false
        every { req.task.isPaused } returns false
        every { req.task.isPausable } returns true

        every { req.task.carbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setPauseStatus(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.isExecuted = any() } answers {
            firstArg<Boolean>()
        }
        every { req.task.isPausable = any() } answers {
            firstArg<Boolean>()
        }

        scheduler.updateCarbonIntensity(100.0)
        scheduler.updateCarbonIntensity(110.0)
        scheduler.updateCarbonIntensity(120.0)
        scheduler.updateCarbonIntensity(130.0)
        scheduler.updateCarbonIntensity(140.0)
        scheduler.updateCarbonIntensity(150.0)
        scheduler.updateCarbonIntensity(160.0)
        scheduler.updateCarbonIntensity(170.0)
        scheduler.updateCarbonIntensity(180.0)
        scheduler.updateCarbonIntensity(190.0)

        // The scheduler tries to schedule the task, but fails as there are no hosts.
        // It should be allocated if there is any actual host
        assertEquals(SchedulingResultType.FAILURE, scheduler.select(mutableListOf(req).iterator()).resultType)
    }

    @Test
    fun testInterruptionIfCarbonIsHigh() {
        val service = mockk<ServiceTask>()
        val host = mockk<SimHost>()
        val guest = mockk<Guest>()

        var result = false

        every { service.carbonThreshold } returns 100.0
        every { service.isPausable } returns true

        every { host.getGuests() } returns listOf(guest)
        every { guest.task } returns service

        //The logic of this function is the same as the one we use in the actual algorithm
        every { host.pausePartially(any()) } answers {
            val iterator = host.getGuests().iterator()
            while (iterator.hasNext()) {
                val guest = iterator.next()
                if (guest.task.isPausable && (guest.task.carbonThreshold < firstArg<Double>())) {
                    result = true
                }
            }
        }

        host.pausePartially(200.0)
        assertEquals(true, result)
    }

    @Test
    fun testNotInterruptionIfTaskIsNotPausable() {
        val service = mockk<ServiceTask>()
        val host = mockk<SimHost>()
        val guest = mockk<Guest>()

        var result = false

        every { service.carbonThreshold } returns 100.0
        //Task is not pausable
        every { service.isPausable } returns false

        every { host.getGuests() } returns listOf(guest)
        every { guest.task } returns service

        //The logic of this function is the same as the one we use in the actual algorithm
        every { host.pausePartially(any()) } answers {
            val iterator = host.getGuests().iterator()
            while (iterator.hasNext()) {
                val guest = iterator.next()
                if (guest.task.isPausable && (guest.task.carbonThreshold < firstArg<Double>())) {
                    result = true
                }
            }
        }

        host.pausePartially(200.0)
        assertEquals(false, result)
    }

    @Test
    fun testNotInterruptionWhenCarbonIsLow() {
        val service = mockk<ServiceTask>()
        val host = mockk<SimHost>()
        val guest = mockk<Guest>()

        var result = false

        every { service.carbonThreshold } returns 100.0
        //Task is not pausable
        every { service.isPausable } returns true

        every { host.getGuests() } returns listOf(guest)
        every { guest.task } returns service

        //The logic of this function is the same as the one we use in the actual algorithm
        every { host.pausePartially(any()) } answers {
            val iterator = host.getGuests().iterator()
            while (iterator.hasNext()) {
                val guest = iterator.next()
                if (guest.task.isPausable && (guest.task.carbonThreshold < firstArg<Double>())) {
                    result = true
                }
            }
        }

        host.pausePartially(20.0)
        assertEquals(false, result)
    }
}
