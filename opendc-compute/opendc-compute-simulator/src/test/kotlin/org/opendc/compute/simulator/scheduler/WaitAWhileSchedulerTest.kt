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
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.compute.simulator.service.TaskNature
import org.opendc.compute.simulator.service.TimeSlot
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.util.LinkedList
import java.util.Queue


class WaitAWhileSchedulerTest {
    @Test
    fun testBasicTimeSlot() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        //1-hour interval
        val forecast: DoubleArray = doubleArrayOf(
            200.0, 200.0, 50.0, 200.0, 200.0, 50.0, 200.0, 200.0)
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
        every { req.task.duration } returns Duration.ofHours(2)
        every { req.task.deadline } returns 3600000 * 8
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(200.0)

        val expectedFirstTimeSlot =
            TimeSlot(50.0,
                clock.instant().plusSeconds(3600 * 3),
                clock.instant().plusSeconds(3600 * 4))

        val expectedSecondTimeSlot =
            TimeSlot(50.0,
                clock.instant().plusSeconds(3600 * 6),
                clock.instant().plusSeconds(3600 * 7))


        val returnedTimeSlotQueue = scheduler.findTimeSlots(req.task, forecast=forecast, req.task.duration.toHours().toInt())

        val calculatedFirstTimeSlot: TimeSlot? = returnedTimeSlotQueue?.remove()
        val calculatedSecondTimeSlot: TimeSlot? = returnedTimeSlotQueue?.remove()

        if (calculatedFirstTimeSlot != null) {
            assertEquals(expectedFirstTimeSlot.startTime, calculatedFirstTimeSlot.startTime)
            assertEquals(expectedFirstTimeSlot.endTime, calculatedFirstTimeSlot.endTime)
            assertEquals(expectedFirstTimeSlot.carbonIntensity, calculatedFirstTimeSlot.carbonIntensity)
        }

        if (calculatedSecondTimeSlot != null) {
            assertEquals(expectedSecondTimeSlot.startTime, calculatedSecondTimeSlot.startTime)
            assertEquals(expectedSecondTimeSlot.endTime, calculatedSecondTimeSlot.endTime)
            assertEquals(expectedSecondTimeSlot.carbonIntensity, calculatedSecondTimeSlot.carbonIntensity)
        }
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
        every { req.task.duration } returns Duration.ofHours(2)
        every { req.task.deadline } returns 8100000
        every { req.task.preScheduled } returns false

        scheduler.updateCarbonIntensity(200.0)

        val expectedStartTime = clock.instant().plus(Duration.ofMillis(0))

        val returnedTimeSlotQueue = scheduler.findTimeSlots(req.task, forecast=forecast, req.task.duration.toHours().toInt())
        val calculatedFirstTimeSlot = returnedTimeSlotQueue?.remove()

        if (calculatedFirstTimeSlot != null) {
            assertEquals(expectedStartTime, calculatedFirstTimeSlot.startTime)
        }
        else {
            assert(false)
        }
    }


    @Test
    fun testNotPauseInCorrectTimeSlot() {

        //first clock for assigning time slot
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochSecond(0)

        //second clock for marking current time
        val clock_2 = mockk<InstantSource>()
        every { clock_2.instant() } returns Instant.ofEpochSecond(3600 * 6 + 10)

        val service = mockk<ServiceTask>()
        val guest = mockk<Guest>()

        val firstTimeSlot =
            TimeSlot(50.0,
                clock.instant().plusSeconds(3600 * 3),
                clock.instant().plusSeconds(3600 * 4))
        val secondTimeSlot =
            TimeSlot(50.0,
                clock.instant().plusSeconds(3600 * 6),
                clock.instant().plusSeconds(3600 * 7))
        val timeSlotQueue: Queue<TimeSlot> = LinkedList()
        timeSlotQueue.add(firstTimeSlot)
        timeSlotQueue.add(secondTimeSlot)

        every { service.timeSlots } returns timeSlotQueue
        every { guest.task } returns service

        var isPaused = false

        val currentTime = clock_2.instant()
        val timeSlot = guest.task.timeSlots
        var currentTimeSlot = timeSlot.peek()
        while (currentTimeSlot != null && currentTime.isAfter(currentTimeSlot.endTime)) {
            currentTimeSlot = timeSlot.remove()
        }

        if (currentTimeSlot == null) {
            isPaused = false
        }
        //Do not interrupt if tasks are still in the located timeslot
        else if (!currentTime.isBefore(currentTimeSlot.startTime) && currentTime.isBefore(currentTimeSlot.endTime)) {
            isPaused = false
        }
        else if (currentTime.isBefore(currentTimeSlot.startTime)) {
            isPaused = true
        }

        //The correct timeslot should be the second one
        assertEquals(secondTimeSlot, currentTimeSlot)
        assertEquals(false, isPaused)
    }

    @Test
    fun testBasicPause() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochSecond(0)

        //second clock for marking current time
        val clock_2 = mockk<InstantSource>()
        every { clock_2.instant() } returns Instant.ofEpochSecond(3600 * 5)

        val service = mockk<ServiceTask>()
        val guest = mockk<Guest>()

        val firstTimeSlot =
            TimeSlot(50.0,
                clock.instant().plusSeconds(3600 * 3),
                clock.instant().plusSeconds(3600 * 4))
        val secondTimeSlot =
            TimeSlot(50.0,
                clock.instant().plusSeconds(3600 * 6),
                clock.instant().plusSeconds(3600 * 7))
        val timeSlotQueue: Queue<TimeSlot> = LinkedList()
        timeSlotQueue.add(firstTimeSlot)
        timeSlotQueue.add(secondTimeSlot)

        every { service.timeSlots } returns timeSlotQueue
        every { guest.task } returns service

        var isPaused = false

        val currentTime = clock_2.instant()
        val timeSlot = guest.task.timeSlots
        var currentTimeSlot = timeSlot.peek()
        while (currentTimeSlot != null && currentTime.isAfter(currentTimeSlot.endTime)) {
            currentTimeSlot = timeSlot.remove()
        }

        if (currentTimeSlot == null) {
            isPaused = false
        }
        //Do not interrupt if tasks are still in the located timeslot
        else if (!currentTime.isBefore(currentTimeSlot.startTime) && currentTime.isBefore(currentTimeSlot.endTime)) {
            isPaused = false
        }
        else if (currentTime.isBefore(currentTimeSlot.startTime)) {
            isPaused = true
        }

        //The correct timeslot should be the second one
        assertEquals(secondTimeSlot, currentTimeSlot)
        //It should be paused because it is not the time yet
        assertEquals(true, isPaused)
    }
}
