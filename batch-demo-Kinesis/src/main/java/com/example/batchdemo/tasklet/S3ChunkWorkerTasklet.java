package com.example.batchdemo.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Worker-side tasklet: lists every chunk file under the S3 chunk
 * output prefix (produced by S3ChunkItemWriter / chunkToS3Job),
 * downloads and parses each one, and inserts rows into target_record.
 * This is the S3-based equivalent of ChunkWorkerTasklet - the
 * mechanism that will run inside a separate ECS worker container.
 */
public class S3ChunkWorkerTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(S3ChunkWorkerTasklet.class);

    private final S3Client s3Client;
    private final String bucketName;
    private final String chunkPrefix;
    private final JdbcTemplate jdbcTemplate;

    public S3ChunkWorkerTasklet(
            S3Client s3Client,
            String bucketName,
            String chunkPrefix,
            DataSource dataSource) {

        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.chunkPrefix = chunkPrefix;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public RepeatStatus execute(
            StepContribution contribution,
            ChunkContext chunkContext) throws Exception {

        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(chunkPrefix)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<S3Object> chunkObjects = listResponse.contents();

        int totalRowsProcessed = 0;

        for (S3Object obj : chunkObjects) {
            log.info("S3ChunkWorkerTasklet: processing s3://{}/{}", bucketName, obj.key());
            totalRowsProcessed += processChunkObject(obj.key());
        }

        log.info("S3ChunkWorkerTasklet: processed {} total row(s) across {} chunk object(s)",
                totalRowsProcessed, chunkObjects.size());

        return RepeatStatus.FINISHED;
    }

    private int processChunkObject(String key) throws Exception {

        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        ResponseInputStream<GetObjectResponse> s3InputStream =
                s3Client.getObject(getRequest);

        int rowCount = 0;

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(s3InputStream))) {

            reader.readLine(); // skip header: sourceId,name,age,processedValue,status,processedAt
            String line;

            while ((line = reader.readLine()) != null) {

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

                rowCount++;
            }
        }

        return rowCount;
    }
}