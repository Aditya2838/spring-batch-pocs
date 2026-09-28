package com.example.batchdemo.writer;

import com.example.batchdemo.domain.TargetRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Writes each processed chunk of TargetRecords as its own CSV file
 * on local disk, under a chunk-output directory. Stand-in for the
 * "chunking" stage that would otherwise upload to S3 - useful for
 * local testing without needing an S3 bucket.
 */
public class DiskChunkItemWriter implements ItemWriter<TargetRecord> {

    private static final Logger log = LoggerFactory.getLogger(DiskChunkItemWriter.class);

    private final Path outputDir;
    private final AtomicInteger chunkCounter = new AtomicInteger(0);

    public DiskChunkItemWriter(String outputDir) {
        this.outputDir = Paths.get(outputDir);
    }

    @Override
    public void write(Chunk<? extends TargetRecord> chunk) throws Exception {

        Files.createDirectories(outputDir);

        // On the very first chunk of a run, clear out any stale files
        // from a previous run to avoid mixing old and new chunk files.
        if (chunkCounter.get() == 0) {
            try (var stream = Files.list(outputDir)) {
                stream.filter(p -> p.toString().endsWith(".csv"))
                        .forEach(p -> p.toFile().delete());
            }
        }

        int index = chunkCounter.getAndIncrement();
        Path chunkFile = outputDir.resolve("chunk_" + index + ".csv");

        try (BufferedWriter writer = Files.newBufferedWriter(chunkFile)) {
            writer.write("sourceId,name,age,processedValue,status,processedAt");
            writer.newLine();

            for (TargetRecord record : chunk) {
                writer.write(record.getSourceId() + "," +
                        record.getName() + "," +
                        record.getAge() + "," +
                        record.getProcessedValue() + "," +
                        record.getStatus() + "," +
                        record.getProcessedAt());
                writer.newLine();
            }
        }

        log.info("DiskChunkItemWriter: wrote {} ({} row(s))",
                chunkFile.toAbsolutePath(), chunk.size());
    }
}