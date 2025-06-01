package org.opendc.compute.simulator.service;

import java.time.Instant;


/**
 This class is used to keep track of time slots assigned to service tasks
 */
public class TimeSlot {

    public Double carbonIntensity;
    public Instant startTime;
    public Instant endTime;

    public TimeSlot(Double carbonIntensity, Instant startTime, Instant endTime) {
        this.carbonIntensity = carbonIntensity;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
