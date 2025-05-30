package org.opendc.compute.simulator.scheduler

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.opendc.compute.simulator.host.SimHost
import org.opendc.compute.simulator.internal.Guest
import org.opendc.compute.simulator.scheduler.timeshift.DTScheduler
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.compute.simulator.service.TaskNature
import java.time.Duration
import java.time.Instant
import java.time.InstantSource

class DTSchedulerTest {

    @Test
    fun testBasicScheduling() {
        val clock = mockk<InstantSource>()

        every { clock.instant() } returns Instant.ofEpochMilli(10)

        val scheduler =
            DTScheduler(
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

        every { req.task.lowerCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.upperCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setExecuted(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.setPauseStatus(any()) } answers {
            firstArg<Boolean>()
        }

        scheduler.updateCarbonIntensity(160.0)
        scheduler.updateCarbonIntensity(170.0)
        scheduler.updateCarbonIntensity(180.0)
        scheduler.updateCarbonIntensity(190.0)
        scheduler.updateCarbonIntensity(110.0)
        scheduler.updateCarbonIntensity(120.0)
        scheduler.updateCarbonIntensity(130.0)
        scheduler.updateCarbonIntensity(140.0)
        scheduler.updateCarbonIntensity(150.0)
        scheduler.updateCarbonIntensity(100.0)

        //It should return FAILURE because it tries to schedule, but there is no machine
        assertEquals(SchedulingResultType.FAILURE, scheduler.select(mutableListOf(req).iterator()).resultType)
    }

    @Test
    fun testBasicScheduleToNonDeferrableTask() {
        val clock = mockk<InstantSource>()

        every { clock.instant() } returns Instant.ofEpochMilli(10)

        val scheduler =
            DTScheduler(
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

        every { req.task.lowerCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.upperCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setExecuted(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.setPauseStatus(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.setPausable(any()) } answers {
            firstArg<Boolean>()
        }

        scheduler.updateCarbonIntensity(110.0)
        scheduler.updateCarbonIntensity(120.0)
        scheduler.updateCarbonIntensity(130.0)
        scheduler.updateCarbonIntensity(140.0)
        scheduler.updateCarbonIntensity(150.0)
        scheduler.updateCarbonIntensity(100.0)
        scheduler.updateCarbonIntensity(160.0)
        scheduler.updateCarbonIntensity(170.0)
        scheduler.updateCarbonIntensity(180.0)
        scheduler.updateCarbonIntensity(190.0)

        //It should return FAILURE because it tries to schedule, but there is no machine
        assertEquals(SchedulingResultType.FAILURE, scheduler.select(mutableListOf(req).iterator()).resultType)
    }




    @Test
    fun testBasicDeferring() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        val scheduler =
            DTScheduler(
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

        every { req.task.lowerCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.upperCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setExecuted(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.setPauseStatus(any()) } answers {
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

        //It should return EMPTY because task should be delayed
        assertEquals(SchedulingResultType.EMPTY, scheduler.select(mutableListOf(req).iterator()).resultType)
    }

    @Test
    fun testRespectDeadline() {
        val clock = mockk<InstantSource>()
        every { clock.instant() } returns Instant.ofEpochMilli(10)

        val scheduler =
            DTScheduler(
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

        every { req.task.lowerCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.upperCarbonThreshold = any() } answers {
            firstArg<Double>()
        }
        every { req.task.setExecuted(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.setPauseStatus(any()) } answers {
            firstArg<Boolean>()
        }
        every { req.task.setPausable(any()) } answers {
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

        //Make the last carbonIntensity the highest
        scheduler.updateCarbonIntensity(200.0)

        //It should return FAILURE because task should be executed
        assertEquals(SchedulingResultType.FAILURE, scheduler.select(mutableListOf(req).iterator()).resultType)
    }


    @Test
    fun testNotInterruptionWhenLowCarbon() {
        val service = mockk<ServiceTask>()
        val host = mockk<SimHost>()
        val guest = mockk<Guest>()

        var isPaused = false

        every { service.upperCarbonThreshold } returns 200.0
        every { service.isPausable } returns true

        every { host.getGuests() } returns listOf(guest)
        every { guest.task } returns service

        //The logic of this function is the same as the one we use in the actual algorithm
        every { host.pausePartially(any()) } answers {
            val iterator = host.getGuests().iterator()
            while (iterator.hasNext()) {
                val guest = iterator.next()
                if (guest.task.isPausable && (guest.task.upperCarbonThreshold < firstArg<Double>())) {
                    isPaused = true
                }
            }
        }

        host.pausePartially(20.0)
        assertEquals(false, isPaused)
    }

    @Test
    fun testInterruptionWhenHighCarbon() {
        val service = mockk<ServiceTask>()
        val host = mockk<SimHost>()
        val guest = mockk<Guest>()

        var isPaused = false

        every { service.upperCarbonThreshold } returns 200.0
        every { service.isPausable } returns true

        every { host.getGuests() } returns listOf(guest)
        every { guest.task } returns service

        //The logic of this function is the same as the one we use in the actual algorithm
        every { host.pausePartially(any()) } answers {
            val iterator = host.getGuests().iterator()
            while (iterator.hasNext()) {
                val guest = iterator.next()
                if (guest.task.isPausable && (guest.task.upperCarbonThreshold < firstArg<Double>())) {
                    isPaused = true
                }
            }
        }

        host.pausePartially(300.0)
        assertEquals(true, isPaused)
    }

    @Test
    fun testNotInterruptionWhenTaskIsNotPausable() {
        val service = mockk<ServiceTask>()
        val host = mockk<SimHost>()
        val guest = mockk<Guest>()

        var isPaused = false

        every { service.upperCarbonThreshold } returns 200.0
        every { service.isPausable } returns false

        every { host.getGuests() } returns listOf(guest)
        every { guest.task } returns service

        //The logic of this function is the same as the one we use in the actual algorithm
        every { host.pausePartially(any()) } answers {
            val iterator = host.getGuests().iterator()
            while (iterator.hasNext()) {
                val guest = iterator.next()
                if (guest.task.isPausable && (guest.task.upperCarbonThreshold < firstArg<Double>())) {
                    isPaused = true
                }
            }
        }

        host.pausePartially(300.0)
        assertEquals(false, isPaused)
    }

}
