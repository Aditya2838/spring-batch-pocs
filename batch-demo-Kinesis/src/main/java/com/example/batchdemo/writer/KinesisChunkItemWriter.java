package com.example.batchdemo.writer;

import com.example.batchdemo.domain.TargetRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.charset.StandardCharsets;

/**
 * Sends each processed TargetRecord as its own record onto a Kinesis
 * stream (one record at a time - streaming, not file-based like
 * S3ChunkItemWriter). A separate consumer reads them off the stream
 * independently.
 */
public class KinesisChunkItemWriter implements ItemWriter<TargetRecord> {

    private static final Logger log = LoggerFactory.getLogger(KinesisChunkItemWriter.class);

    private final KinesisClient kinesisClient;
    private final String streamName;

    public KinesisChunkItemWriter(KinesisClient kinesisClient, String streamName) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
    }

    @Override
    public void write(Chunk<? extends TargetRecord> chunk) {

        for (TargetRecord record : chunk) {

            String csvLine = record.getSourceId() + "," +
                    record.getName() + "," +
                    record.getAge() + "," +
                    record.getProcessedValue() + "," +
                    record.getStatus() + "," +
                    record.getProcessedAt();

            PutRecordRequest request = PutRecordRequest.builder()
                    .streamName(streamName)
                    .partitionKey(String.valueOf(record.getSourceId()))
                    .data(SdkBytes.fromByteArray(csvLine.getBytes(StandardCharsets.UTF_8)))
                    .build();

            kinesisClient.putRecord(request);

            log.info("KinesisChunkItemWriter: put record for sourceId={} onto stream {}",
                    record.getSourceId(), streamName);
        }
    }
}