package com.example.batchdemo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Explicitly launches all batch jobs at application startup, in order.
 * Replaces the fragile spring.batch.job.names property-based mechanism,
 * which doesn't reliably auto-run multiple jobs across Spring Boot setups.
 */
@Component
public class StartupJobRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(StartupJobRunner.class);

    private final JobLauncher jobLauncher;
    private final Job recordProcessingJob;
    private final Job fileChunkingJob;
    private final Job chunkToS3Job;
    private final Job chunkWorkerJob;

    public StartupJobRunner(
            JobLauncher jobLauncher,
            @Qualifier("recordProcessingJob") Job recordProcessingJob,
            @Qualifier("fileChunkingJob") Job fileChunkingJob,
            @Qualifier("chunkToS3Job") Job chunkToS3Job,
            @Qualifier("chunkWorkerJob") Job chunkWorkerJob) {

        this.jobLauncher = jobLauncher;
        this.recordProcessingJob = recordProcessingJob;
        this.fileChunkingJob = fileChunkingJob;
        this.chunkToS3Job = chunkToS3Job;
        this.chunkWorkerJob = chunkWorkerJob;
    }

    @Override
    public void run(String... args) throws Exception {

        JobParameters params1 = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        log.info("StartupJobRunner: launching recordProcessingJob");
        jobLauncher.run(recordProcessingJob, params1);

        JobParameters params2 = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        log.info("StartupJobRunner: launching fileChunkingJob");
        jobLauncher.run(fileChunkingJob, params2);

        // chunkToS3Job must run BEFORE chunkWorkerJob —
        // the worker reads whatever files the chunker just produced.
        JobParameters params3 = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        log.info("StartupJobRunner: launching chunkToS3Job");
        jobLauncher.run(chunkToS3Job, params3);

        JobParameters params4 = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        log.info("StartupJobRunner: launching chunkWorkerJob");
        jobLauncher.run(chunkWorkerJob, params4);

        log.info("StartupJobRunner: all jobs completed");
    }
}