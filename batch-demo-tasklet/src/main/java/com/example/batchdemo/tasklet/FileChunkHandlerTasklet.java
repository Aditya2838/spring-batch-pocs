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
import java.time.LocalDateTime;
import java.util.List;

/**
 * Task-based tasklet that manually reads every small chunk file produced
 * by FileChunkingTasklet, processes each row (uppercase name), and
 * inserts it into target_records_task — all done by hand, without a
 * chunk-oriented reader/processor/writer.
 */
public class FileChunkHandlerTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(FileChunkHandlerTasklet.class);

    private final String chunkOutputDir;
    private final JdbcTemplate jdbcTemplate;

    public FileChunkHandlerTasklet(String chunkOutputDir, DataSource dataSource) {
        this.chunkOutputDir = chunkOutputDir;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public RepeatStatus execute(
            StepContribution contribution,
            ChunkContext chunkContext) throws Exception {

        Path dir = Paths.get(chunkOutputDir);

        List<Path> chunkFiles;
        try (var stream = Files.list(dir)) {
            chunkFiles = stream
                    .filter(p -> p.toString().endsWith(".csv"))
                    .sorted()
                    .toList();
        }

        int totalRowsProcessed = 0;

        for (Path chunkFile : chunkFiles) {
            log.info("FileChunkHandlerTasklet: handling chunk file {}", chunkFile.getFileName());
            totalRowsProcessed += handleSingleChunkFile(chunkFile);
        }

        log.info("FileChunkHandlerTasklet: processed {} total row(s) across {} chunk file(s)",
                totalRowsProcessed, chunkFiles.size());

        return RepeatStatus.FINISHED;
    }

    private int handleSingleChunkFile(Path chunkFile) throws IOException {

        int rowCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(chunkFile)) {

            reader.readLine(); // skip header (id,name,age)
            String line;

            while ((line = reader.readLine()) != null) {

                String[] fields = line.split(",");
                Integer sourceId = Integer.parseInt(fields[0].trim());
                String name = fields[1].trim();
                Integer age = Integer.parseInt(fields[2].trim());
                String uppercaseName = name.toUpperCase();

                jdbcTemplate.update(
                        "INSERT INTO target_records_task " +
                        "(source_id, name, age, processed_value, status, processed_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        sourceId,
                        name,
                        age,
                        uppercaseName,
                        "PROCESSED",
                        Timestamp.valueOf(LocalDateTime.now())
                );

                rowCount++;
            }
        }

        return rowCount;
    }
}