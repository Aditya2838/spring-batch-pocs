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
 * Explicitly launches both batch jobs at application startup, in order.
 * Replaces the fragile spring.batch.job.names property-based mechanism,
 * which doesn't reliably auto-run multiple jobs across Spring Boot setups.
 */
@Component
public class StartupJobRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(StartupJobRunner.class);

    private final JobLauncher jobLauncher;
    private final Job recordProcessingJob;
    private final Job fileChunkingJob;

    public StartupJobRunner(
            JobLauncher jobLauncher,
            @Qualifier("recordProcessingJob") Job recordProcessingJob,
            @Qualifier("fileChunkingJob") Job fileChunkingJob) {

        this.jobLauncher = jobLauncher;
        this.recordProcessingJob = recordProcessingJob;
        this.fileChunkingJob = fileChunkingJob;
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

        log.info("StartupJobRunner: both jobs completed");
    }
}