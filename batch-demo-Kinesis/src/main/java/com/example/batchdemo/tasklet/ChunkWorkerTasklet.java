package com.example.batchdemo.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;

/**
 * Worker-side tasklet: reads every chunk file left behind by
 * chunkToDiskJob (DiskChunkItemWriter), parses each row, and
 * inserts it into target_record. This simulates what an ECS
 * worker container will eventually do when it picks up a chunk
 * that a separate chunking process produced.
 */
public class ChunkWorkerTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(ChunkWorkerTasklet.class);

    private final String chunkOutputDir;
    private final JdbcTemplate jdbcTemplate;

    public ChunkWorkerTasklet(String chunkOutputDir, DataSource dataSource) {
        this.chunkOutputDir = chunkOutputDir;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public RepeatStatus execute(
            StepContribution contribution,
            ChunkContext chunkContext) throws Exception {

        Path dir = Paths.get(chunkOutputDir);

        if (!Files.exists(dir)) {
            log.warn("ChunkWorkerTasklet: chunk directory {} does not exist, nothing to process",
                    dir.toAbsolutePath());
            return RepeatStatus.FINISHED;
        }

        List<Path> chunkFiles;
        try (var stream = Files.list(dir)) {
            chunkFiles = stream
                    .filter(p -> p.toString().endsWith(".csv"))
                    .sorted()
                    .toList();
        }

        int totalRowsProcessed = 0;

        for (Path chunkFile : chunkFiles) {
            log.info("ChunkWorkerTasklet: processing {}", chunkFile.getFileName());
            totalRowsProcessed += processChunkFile(chunkFile);
        }

        log.info("ChunkWorkerTasklet: processed {} total row(s) across {} chunk file(s)",
                totalRowsProcessed, chunkFiles.size());

        return RepeatStatus.FINISHED;
    }

    private int processChunkFile(Path chunkFile) throws IOException {

        int rowCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(chunkFile)) {

            reader.readLine(); // skip header: sourceId,name,age,processedValue,status,processedAt
            String line;

            while ((line = reader.readLine()) != null) {

                String[] fields = line.split(",");
                Integer sourceId = Integer.parseInt(fields[0].trim());
                String name = fields[1].trim();
                Integer age = Integer.parseInt(fields[2].trim());
                String processedValue = fields[3].trim();
                String status = fields[4].trim();
                // fields[5] = processedAt from the chunk file — we timestamp fresh instead

                jdbcTemplate.update(
                        "INSERT INTO target_record " +
                        "(source_id, name, age, processed_value, status, processed_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        sourceId,
                        name,
                        age,
                        processedValue,
                        status,
                        Timestamp.valueOf(java.time.LocalDateTime.now())
                );

                rowCount++;
            }
        }

        return rowCount;
    }
}