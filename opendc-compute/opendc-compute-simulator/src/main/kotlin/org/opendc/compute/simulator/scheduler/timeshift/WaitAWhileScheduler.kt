package org.opendc.compute.simulator.scheduler.timeshift

import org.opendc.compute.api.TaskState
import org.opendc.compute.simulator.scheduler.ComputeScheduler
import org.opendc.compute.simulator.scheduler.SchedulingRequest
import org.opendc.compute.simulator.scheduler.SchedulingResult
import org.opendc.compute.simulator.scheduler.SchedulingResultType
import org.opendc.compute.simulator.scheduler.filters.HostFilter
import org.opendc.compute.simulator.scheduler.weights.HostWeigher
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.compute.simulator.service.TimeSlot
import org.opendc.simulator.compute.power.CarbonModel
import java.time.Instant
import java.time.InstantSource
import java.util.LinkedList
import java.util.Queue
import java.util.SplittableRandom
import java.util.random.RandomGenerator
import kotlin.math.min
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

public class WaitAWhileScheduler(
    private val filters: List<HostFilter>,
    private val weighers: List<HostWeigher>,
    override val windowSize: Int,
    override val clock: InstantSource,
    private val subsetSize: Int = 1,
    override val forecast: Boolean = true,
    override val shortForecastThreshold: Double = 0.2,
    override val longForecastThreshold: Double = 0.35,
    override val forecastSize: Int = 24,
    private val random: RandomGenerator = SplittableRandom(0),

    ) : ComputeScheduler, Timeshifter {

    override val pastCarbonIntensities: LinkedList<Double> = LinkedList<Double>()
    override var carbonRunningSum: Double = 0.0
    override var shortLowCarbon: Boolean = false // Low carbon regime for short tasks (< 2 hours)
    override var longLowCarbon: Boolean = false // Low carbon regime for long tasks (>= hours)
    override var carbonMod: CarbonModel? = null

    /**
    My newly added variable for carbon tracing
     */
    override var currentCarbonIntensity: Double = 0.0

    private val hosts = mutableListOf<HostView>()

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(iter: MutableIterator<SchedulingRequest>): SchedulingResult {
        var result: SchedulingResult? = null

        for (request in iter) {
            if (request.isCancelled) {
                iter.remove()
                continue
            }

            val task = request.task
            val currentTime = clock.instant()

            if (task.preScheduled) {
                //If it is not the time, then we keep it waiting
                val currentSlot = task.currentTimeSlot
                if (currentTime.isBefore(currentSlot.startTime)) {
                    continue
                }
                else if (currentTime.isAfter(currentSlot.startTime) && currentTime.isBefore(currentSlot.endTime)) {
                    //Execute now
                }
            } else if (task.nature.deferrable) {
                val taskDurationInHours = task.duration.toHours().toInt()
                val deadline = Instant.ofEpochMilli(task.deadline)
                val timeToDeadlineInHours = java.time.Duration.between(currentTime, deadline).toHours()

                var forecast: DoubleArray? = null
                if (timeToDeadlineInHours.toInt() > 0) {
                    forecast = carbonMod!!.getForecast(timeToDeadlineInHours.toInt())
                }

                if (forecast != null && taskDurationInHours < timeToDeadlineInHours) {
                    val selectedTimeSlots = findTimeSlots(task, forecast, taskDurationInHours)
                    task.timeSlots = selectedTimeSlots
                    val firstTimeSlot = task.timeSlots.peek()
                    if (firstTimeSlot?.startTime == currentTime) {
                        task.preScheduled = true
                    }
                    else {
                        task.preScheduled = true
                        continue
                    }
                }
            }

            val filteredHosts = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }

            val subset =
                if (weighers.isNotEmpty()) {
                    val results = weighers.map { it.getWeights(filteredHosts, task) }
                    val weights = DoubleArray(filteredHosts.size)

                    for (result in results) {
                        val min = result.min
                        val range = (result.max - min)

                        // Skip result if all weights are the same
                        if (range == 0.0) {
                            continue
                        }

                        val multiplier = result.multiplier
                        val factor = multiplier / range

                        for ((i, weight) in result.weights.withIndex()) {
                            weights[i] += factor * (weight - min)
                        }
                    }

                    weights.indices
                        .asSequence()
                        .sortedByDescending { weights[it] }
                        .map { filteredHosts[it] }
                        .take(subsetSize)
                        .toList()
                } else {
                    filteredHosts
                }

            val maxSize = min(subsetSize, subset.size)
            if (maxSize == 0) {
                result = SchedulingResult(SchedulingResultType.FAILURE, null, request)
                break
            } else {
                iter.remove()
                result = SchedulingResult(SchedulingResultType.SUCCESS, subset[random.nextInt(maxSize)], request)
                break
            }
        }
        if (result == null) return SchedulingResult(SchedulingResultType.EMPTY)
        return result
    }

    override fun removeTask(
        task: ServiceTask,
        host: HostView?,
    ) {
    }

    public fun findTimeSlots(task: ServiceTask, forecast: DoubleArray, taskDurationInHours: Int): Queue<TimeSlot>? {
        val currentTime = clock.instant()
        val oneHourDuration = java.time.Duration.ofHours(1)
        val currentTimeSlot = TimeSlot(currentCarbonIntensity, currentTime, currentTime.plus(oneHourDuration))
        val timeSlotQueue: Queue<TimeSlot> = LinkedList()

        if (task.nature.deferrable) {
            val availableSlots = mutableListOf<TimeSlot>()
            availableSlots.add(currentTimeSlot)
            var startTime = currentTime.plus(oneHourDuration)
            var endTime = startTime.plus(oneHourDuration)

            for (carbon in forecast) {
                availableSlots.add(TimeSlot(carbon, startTime, endTime))
                startTime = startTime.plus(oneHourDuration)
                endTime = endTime.plus(oneHourDuration)
            }

            val sortedByIntensity = availableSlots.sortedBy { it.carbonIntensity }
            var selectedSlots = sortedByIntensity.take(taskDurationInHours)
            if (selectedSlots.isEmpty()) {
                selectedSlots = sortedByIntensity.take(1)
            }

            val sortedByTime = selectedSlots.sortedBy { it.startTime }
            for (slot in sortedByTime) {
                timeSlotQueue.add(slot)
            }
            return timeSlotQueue
        }

        timeSlotQueue.add(currentTimeSlot)
        return timeSlotQueue
    }
}
