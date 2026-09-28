package com.example.batchdemo.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Simple polling consumer: reads whatever records are currently on
 * the Kinesis stream (starting from TRIM_HORIZON - the oldest
 * available record) and inserts each into target_record. This is
 * the streaming equivalent of S3ChunkWorkerTasklet.
 */
public class KinesisConsumerTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(KinesisConsumerTasklet.class);

    private final KinesisClient kinesisClient;
    private final String streamName;
    private final JdbcTemplate jdbcTemplate;

    public KinesisConsumerTasklet(
            KinesisClient kinesisClient,
            String streamName,
            DataSource dataSource) {

        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public RepeatStatus execute(
            StepContribution contribution,
            ChunkContext chunkContext) throws Exception {

        DescribeStreamResponse streamInfo = kinesisClient.describeStream(
                DescribeStreamRequest.builder().streamName(streamName).build());

        List<Shard> shards = streamInfo.streamDescription().shards();

        int totalRowsProcessed = 0;

        for (Shard shard : shards) {

            GetShardIteratorResponse iteratorResponse = kinesisClient.getShardIterator(
                    GetShardIteratorRequest.builder()
                            .streamName(streamName)
                            .shardId(shard.shardId())
                            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                            .build());

            String shardIterator = iteratorResponse.shardIterator();

            // Poll a few times to drain what's currently available
            for (int attempt = 0; attempt < 3 && shardIterator != null; attempt++) {

                GetRecordsResponse recordsResponse = kinesisClient.getRecords(
                        GetRecordsRequest.builder()
                                .shardIterator(shardIterator)
                                .limit(100)
                                .build());

                for (Record record : recordsResponse.records()) {
                    processRecord(record);
                    totalRowsProcessed++;
                }

                shardIterator = recordsResponse.nextShardIterator();

                if (recordsResponse.records().isEmpty()) {
                    break; // nothing more waiting right now
                }
            }
        }

        log.info("KinesisConsumerTasklet: processed {} total record(s) from stream {}",
                totalRowsProcessed, streamName);

        return RepeatStatus.FINISHED;
    }

    private void processRecord(Record record) {

        SdkBytes data = record.data();
        String line = new String(data.asByteArray(), StandardCharsets.UTF_8);

        String[] fields = line.split(",");
        Integer sourceId = Integer.parseInt(fields[0].trim());
        String name = fields[1].trim();
        Integer age = Integer.parseInt(fields[2].trim());
        String processedValue = fields[3].trim();
        String status = fields[4].trim();

        jdbcTemplate.update(
                "INSERT INTO target_record " +
                "(source_id, name, age, processed_value, status, processed_at) " +
                "VALUES (?, ?, ?, ?, ?, ?)",
                sourceId,
                name,
                age,
                processedValue,
                status,
                Timestamp.valueOf(LocalDateTime.now())
        );

        log.info("KinesisConsumerTasklet: inserted sourceId={}", sourceId);
    }
}