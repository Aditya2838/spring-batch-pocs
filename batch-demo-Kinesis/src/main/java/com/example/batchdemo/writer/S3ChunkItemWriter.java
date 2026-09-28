package com.example.batchdemo.writer;

import com.example.batchdemo.domain.TargetRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Uploads each processed chunk of TargetRecords as its own CSV file
 * in S3, under a chunk-output prefix. This is the "chunking" stage:
 * one big source file becomes many small chunk files in S3, ready
 * for separate worker processes/containers to pick up independently.
 */
public class S3ChunkItemWriter implements ItemWriter<TargetRecord> {

    private static final Logger log = LoggerFactory.getLogger(S3ChunkItemWriter.class);

    private final S3Client s3Client;
    private final String bucketName;
    private final String outputPrefix;
    private final AtomicInteger chunkCounter = new AtomicInteger(0);

    public S3ChunkItemWriter(
            S3Client s3Client,
            String bucketName,
            String outputPrefix) {

        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.outputPrefix = outputPrefix;
    }

    @Override
    public void write(Chunk<? extends TargetRecord> chunk) {

        int index = chunkCounter.getAndIncrement();
        String key = outputPrefix + "/chunk_" + index + ".csv";

        StringBuilder csv = new StringBuilder();
        csv.append("sourceId,name,age,processedValue,status,processedAt\n");

        for (TargetRecord record : chunk) {
            csv.append(record.getSourceId()).append(",")
               .append(record.getName()).append(",")
               .append(record.getAge()).append(",")
               .append(record.getProcessedValue()).append(",")
               .append(record.getStatus()).append(",")
               .append(record.getProcessedAt()).append("\n");
        }

        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build(),
                RequestBody.fromString(csv.toString(), StandardCharsets.UTF_8)
        );

        log.info("S3ChunkItemWriter: uploaded {} ({} row(s)) to bucket {}",
                key, chunk.size(), bucketName);
    }
}