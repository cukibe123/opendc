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
import org.opendc.simulator.compute.power.CarbonModel
import java.time.Instant
import java.time.InstantSource
import java.util.LinkedList
import java.util.SplittableRandom
import java.util.random.RandomGenerator
import kotlin.math.min
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

public class WaitAWhileScheduler(
    private val filters: List<HostFilter>,
    private val weighers: List<HostWeigher>,
    override val windowSize: Int,
    override val clock : InstantSource,
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
    override var lowerThreshold: Double = 0.0
    override var upperThreshold: Double = 0.0

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

            if (task.preScheduled) {
                //If it is not the time, then we keep it waiting
                val currentTime = clock.instant()
                if (currentTime.isBefore(task.scheduledTime)) {
                    continue
                }
            }
            else if (task.nature.deferrable) {
                val currentTime = clock.instant()
                val taskDurationInHours = task.duration.toHours().toInt()
                val deadline = Instant.ofEpochMilli(task.deadline)
                val timeToDeadlineInHours = java.time.Duration.between(currentTime, deadline).toHours()
                val forecast = carbonMod!!.getForecast(timeToDeadlineInHours.toInt())

                //Implement logic for choosing best time window here
                if (forecast != null && taskDurationInHours < timeToDeadlineInHours) {
                    var estimatedDelayTime = 0
                    var lowestWindow = 0.0
                    for (i in 0 until forecast.size - taskDurationInHours - 1) {
                        val range = forecast.copyOfRange(i, i + taskDurationInHours)
                        val currentWindow = range.average()
                        if (lowestWindow == 0.0) {
                            lowestWindow = currentWindow
                            continue
                        }
                        else {
                            if (currentWindow < lowestWindow) {
                                lowestWindow = currentWindow
                                estimatedDelayTime = i
                            }
                        }
                    }
                    val estimatedDelayTimeInDuration = estimatedDelayTime.hours
                    val estimatedExecutionTime = currentTime.plus(estimatedDelayTimeInDuration.toJavaDuration())
                    task.setScheduledTime(estimatedExecutionTime)
                    task.setPreScheduled(true)
                }
            }

            //We check if the task is deferrable, and the best time to execute is later
            //If it is not deferrable or it is best to execute now, then we do not run this if-statement

            val filteredHosts = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }

            /**
             * At this part, we consider if the targeted task is paused or not
             * If it is paused, we consider the carbon upperbound at the time it was paused
             * If the current carbon intensity is lower than the lowerbound, then we are good to schedule
             * Otherwise, we wait further
             * If the deadline allows, we proceed to the stage of delaying. If not, we must schedule tasks right now
             */

            /**
             * If tasks can be scheduled right now, we must update the state of ServiceTask, changing the lowerbound
             * carbon to null
             */

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
}
