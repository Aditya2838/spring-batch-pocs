package com.example.batchdemo.partition;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class IdRangePartitioner implements Partitioner {

    private static final int MIN_ID = 1;
    private static final int MAX_ID = 1000;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();

        int totalRange = MAX_ID - MIN_ID + 1;
        int rangeSize = totalRange / gridSize;

        int start = MIN_ID;
        for (int i = 0; i < gridSize; i++) {
            int end = (i == gridSize - 1) ? MAX_ID : (start + rangeSize - 1);

            ExecutionContext context = new ExecutionContext();
            context.putInt("minId", start);
            context.putInt("maxId", end);
            partitions.put("partition" + i, context);

            start = end + 1;
        }

        return partitions;
    }
}
