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

public class DTScheduler(
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


            if (task.pauseStatus == true && task.pausable == true) {
                if (lowerThreshold < currentCarbonIntensity) {
                    val currentTime = clock.instant()
                    val estimatedCompletion = currentTime.plus(task.duration)
                    val deadline = Instant.ofEpochMilli(task.deadline)
                    if (estimatedCompletion.isBefore(deadline)) {
                        continue
                    }
                    //If the deadline is not allowed, we must proceed
                    //Must update the pauseStatus if the task continues
                    //Add one more variable to control the interrupts
                    //Add threshold for interrupts
                    task.pausable = false
                }
            }
            else if (task.pausable == false) {}
            else {
                if (task.nature.deferrable) {
                    if (upperThreshold < currentCarbonIntensity) {
                        val currentTime = clock.instant()
                        val estimatedCompletion = currentTime.plus(task.duration)
                        val deadline = Instant.ofEpochMilli(task.deadline)
                        if (estimatedCompletion.isBefore(deadline)) {
                            // No need to schedule this task in a high carbon intensity period
                            continue
                        }
                        task.pausable = false
                    }
                }
            }

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
