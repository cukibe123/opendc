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
import org.opendc.compute.simulator.scheduler.timeshift.WaitAWhileScheduler
import org.opendc.compute.simulator.service.TaskNature
import java.time.Duration
import java.time.Instant
import java.time.InstantSource


class WaitAWhileSchedulerTest {
    @Test
    fun testBasicScheduledTime() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        //15-minute interval
        //Test with 15 because it depends on the given carbon traces
        val forecast: DoubleArray = doubleArrayOf(
            200.0, 200.0, 50.0, 100.0, 100.0, 100.0, 200.0, 200.0)
        val scheduler =
            WaitAWhileScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 2,
                forecastSize = 4,
                clock = clock,
                forecast = false,
                //It does not change the behaviour
                //Set to false so we don't have to call forecast from CarbonModel
            )
        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofHours(4)
        every { req.task.deadline } returns 3600000 * 8
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(200.0)

        val expectedScheduledTime = clock.instant().plus(Duration.ofMillis(3600000 * 3))
        assertEquals(expectedScheduledTime, scheduler.findBestWindow(req.task, forecast=forecast, req.task.duration.toHours().toInt()))
    }

    @Test
    fun testScheduleNowIfNonDeferrableTask() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        //15-minute interval
        //Test with 15 because it depends on the given carbon traces
        val forecast: DoubleArray = doubleArrayOf(
            200.0, 200.0, 50.0, 100.0, 100.0, 100.0, 200.0, 200.0)
        val scheduler =
            WaitAWhileScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 2,
                forecastSize = 4,
                clock = clock,
                forecast = false,
                //It does not change the behaviour
                //Set to false so we don't have to call forecast from CarbonModel
            )
        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(false)
        every { req.task.duration } returns Duration.ofMillis(900000)
        every { req.task.deadline } returns 8100000
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(200.0)

        val expectedScheduledTime = clock.instant().plus(Duration.ofMillis(0))
        assertEquals(expectedScheduledTime, scheduler.findBestWindow(req.task, forecast=forecast, req.task.duration.toHours().toInt()))
    }


    @Test
    fun testNowIsTheBestTime() { //Now is the best time
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        //15-minute interval
        //Test with 15 because it depends on the given carbon traces
        val forecast: DoubleArray = doubleArrayOf(
            50.0, 50.0, 100.0, 100.0, 200.0, 200.0, 200.0, 200.0)
        val scheduler =
            WaitAWhileScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 2,
                forecastSize = 4,
                clock = clock,
                forecast = false,
                //It does not change the behaviour
                //Set to false so we don't have to call forecast from CarbonModel
            )
        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofMillis(7200000)
        every { req.task.deadline } returns 3600000 * 8
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(50.0)

        //now equals to clock.instant()
        assertEquals(clock.instant(), scheduler.findBestWindow(req.task, forecast=forecast, req.task.duration.toHours().toInt()))
    }

    @Test
    fun testNextBlockIsTheBest() { //Next time window is the best time
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        //15-minute interval
        //Test with 15 because it depends on the given carbon trace
        val forecast: DoubleArray = doubleArrayOf(
            50.0, 50.0, 100.0, 100.0, 200.0, 200.0, 200.0, 200.0)
        val scheduler =
            WaitAWhileScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 2,
                forecastSize = 4,
                clock = clock,
                forecast = false,
                //It does not change the behaviour
                //Set to false so we don't have to call forecast from CarbonModel
            )
        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofMillis(3600000)
        every { req.task.deadline } returns 3600000 * 8
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(70.0)
        val expectedTime = clock.instant().plus(Duration.ofMillis(3600000))
        assertEquals(expectedTime, scheduler.findBestWindow(req.task, forecast=forecast, req.task.duration.toHours().toInt()))
    }

    @Test
    fun testLastWindowIsTheBestTime() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        //15-minute interval
        //Test with 15 because it depends on the given carbon traces
        val forecast: DoubleArray = doubleArrayOf(
            200.0, 200.0, 200.0, 200.0, 50.0, 100.0, 100.0, 100.0)
        val scheduler =
            WaitAWhileScheduler(
                filters = emptyList(),
                weighers = emptyList(),
                windowSize = 2,
                forecastSize = 4,
                clock = clock,
                forecast = false,
                //It does not change the behaviour
                //Set to false so we don't have to call forecast from CarbonModel
            )
        val req = mockk<SchedulingRequest>()
        every { req.task.flavor.coreCount } returns 2
        every { req.task.flavor.memorySize } returns 1024
        every { req.isCancelled } returns false
        every { req.task.nature } returns TaskNature(true)
        every { req.task.duration } returns Duration.ofMillis(3600000 * 2)
        every { req.task.deadline } returns 3600000 * 8
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(200.0)

        val expectedScheduledTime = clock.instant().plus(Duration.ofMillis(3600000 * 5))
        assertEquals(expectedScheduledTime, scheduler.findBestWindow(req.task, forecast=forecast, req.task.duration.toHours().toInt()))
    }
}
