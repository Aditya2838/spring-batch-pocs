package com.example.batchdemo.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Task-based tasklet that manually downloads the S3 CSV and splits it
 * into several smaller "chunk files" on local disk, to be handled
 * individually by FileChunkHandlerTasklet.
 */
public class FileChunkingTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(FileChunkingTasklet.class);

    private static final int LINES_PER_CHUNK_FILE = 3;

    private final S3Client s3Client;
    private final String bucketName;
    private final String objectKey;
    private final String chunkOutputDir;

    public FileChunkingTasklet(
            S3Client s3Client,
            String bucketName,
            String objectKey,
            String chunkOutputDir) {

        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.chunkOutputDir = chunkOutputDir;
    }

    @Override
    public RepeatStatus execute(
            StepContribution contribution,
            ChunkContext chunkContext) throws Exception {

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        ResponseInputStream<GetObjectResponse> s3InputStream =
                s3Client.getObject(request);

        Path outputDir = Paths.get(chunkOutputDir);
        clearDirectory(outputDir);
        Files.createDirectories(outputDir);

        int chunkIndex = 0;

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(s3InputStream))) {

            String header = reader.readLine();
            String line;
            List<String> buffer = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                buffer.add(line);

                if (buffer.size() == LINES_PER_CHUNK_FILE) {
                    writeChunkFile(outputDir, chunkIndex++, header, buffer);
                    buffer.clear();
                }
            }

            if (!buffer.isEmpty()) {
                writeChunkFile(outputDir, chunkIndex++, header, buffer);
            }
        }

        log.info("FileChunkingTasklet: split source file into {} chunk file(s) in {}",
                chunkIndex, outputDir.toAbsolutePath());

        return RepeatStatus.FINISHED;
    }

    private void clearDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            try (var stream = Files.walk(dir)) {
                stream.sorted((a, b) -> b.compareTo(a))
                        .forEach(p -> p.toFile().delete());
            }
        }
    }

    private void writeChunkFile(
            Path outputDir,
            int chunkIndex,
            String header,
            List<String> lines) throws IOException {

        Path chunkFile = outputDir.resolve("chunk_" + chunkIndex + ".csv");

        try (BufferedWriter writer = Files.newBufferedWriter(chunkFile)) {
            writer.write(header);
            writer.newLine();
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }

        log.info("FileChunkingTasklet: wrote {} ({} row(s))",
                chunkFile.getFileName(), lines.size());
    }
}