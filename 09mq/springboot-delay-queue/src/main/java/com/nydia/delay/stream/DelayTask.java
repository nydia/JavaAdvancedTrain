package com.nydia.delay.stream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DelayTask implements Serializable, Delayed {

    private String taskId;
    private String payload;
    private long executeTime;
    private long addTime;

    public DelayTask(String taskId, String payload, long delayMillis) {
        this.taskId = taskId;
        this.payload = payload;
        this.addTime = System.currentTimeMillis();
        this.executeTime = System.currentTimeMillis() + delayMillis;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(executeTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.executeTime, ((DelayTask) o).executeTime);
    }

    public boolean isReady() {
        return System.currentTimeMillis() >= executeTime;
    }
}