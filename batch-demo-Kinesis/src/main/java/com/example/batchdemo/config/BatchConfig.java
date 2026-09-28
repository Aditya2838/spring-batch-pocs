package com.example.batchdemo.config;

import com.example.batchdemo.domain.SourceRecord;
import com.example.batchdemo.domain.TargetRecord;
import com.example.batchdemo.processor.RecordUpperCaseProcessor;
import com.example.batchdemo.reader.S3CsvItemReader;
import com.example.batchdemo.writer.TargetRecordItemWriter;
import com.example.batchdemo.writer.S3ChunkItemWriter;
import com.example.batchdemo.writer.KinesisChunkItemWriter;
import com.example.batchdemo.tasklet.KinesisConsumerTasklet;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import com.example.batchdemo.tasklet.FileChunkingTasklet;
import com.example.batchdemo.tasklet.FileChunkHandlerTasklet;
import com.example.batchdemo.tasklet.ChunkWorkerTasklet;
import com.example.batchdemo.tasklet.S3ChunkWorkerTasklet;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class BatchConfig {

    private static final int CHUNK_SIZE = 5;

    // ---------- Existing chunk-oriented job (unchanged) ----------

    @Bean
    public Job recordProcessingJob(
            JobRepository jobRepository,
            Step processingStep,
            Step reportStep) {

        return new JobBuilder("recordProcessingJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(processingStep)
                .next(reportStep)
                .build();
    }

    @Bean
    @StepScope
    public ItemStreamReader<SourceRecord> sourceRecordItemReader(
            S3Client s3Client,
            @Value("${aws.s3.bucket-name}") String bucket,
            @Value("${aws.s3.input-key}") String key) {

        return new S3CsvItemReader(
                s3Client,
                bucket,
                key
        );
    }

    @Bean
    public ItemProcessor<SourceRecord, TargetRecord>
    recordUpperCaseProcessor() {

        return new RecordUpperCaseProcessor();
    }

    @Bean
    public ItemWriter<TargetRecord> targetRecordItemWriter(
            DataSource dataSource) {

        return TargetRecordItemWriter.build(dataSource);
    }

    @Bean
    public Step processingStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemStreamReader<SourceRecord> sourceRecordItemReader,
            ItemProcessor<SourceRecord, TargetRecord>
                    recordUpperCaseProcessor,
            ItemWriter<TargetRecord> targetRecordItemWriter) {

        return new StepBuilder("processingStep", jobRepository)
                .<SourceRecord, TargetRecord>chunk(CHUNK_SIZE, transactionManager)
                .reader(sourceRecordItemReader)
                .processor(recordUpperCaseProcessor)
                .writer(targetRecordItemWriter)
                .build();
    }

    @Bean
    public Step reportStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager) {

        return new StepBuilder("reportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    System.out.println(
                            "S3 CSV processing completed successfully"
                    );

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // ---------- New task-based (Tasklet) file-chunking job ----------

    @Bean
    public Job fileChunkingJob(
            JobRepository jobRepository,
            Step chunkFileStep,
            Step handleFileChunkStep) {

        return new JobBuilder("fileChunkingJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(chunkFileStep)
                .next(handleFileChunkStep)
                .build();
    }

    @Bean
    public Step chunkFileStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            S3Client s3Client,
            @Value("${aws.s3.bucket-name}") String bucket,
            @Value("${aws.s3.input-key}") String key,
            @Value("${file.chunk.output-dir}") String chunkOutputDir) {

        return new StepBuilder("chunkFileStep", jobRepository)
                .tasklet(
                        new FileChunkingTasklet(s3Client, bucket, key, chunkOutputDir),
                        transactionManager)
                .build();
    }

    @Bean
    public Step handleFileChunkStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            DataSource dataSource,
            @Value("${file.chunk.output-dir}") String chunkOutputDir) {

        return new StepBuilder("handleFileChunkStep", jobRepository)
                .tasklet(
                        new FileChunkHandlerTasklet(chunkOutputDir, dataSource),
                        transactionManager)
                .build();
    }

    // ---------- Pattern 6, Stage 1: chunk processed output to S3 ----------
    // (Simulates the "ECS Spring Batch chunking" stage feeding S3,
    // ahead of separate worker containers picking chunks up.)

    @Bean
    public Job chunkToS3Job(
            JobRepository jobRepository,
            Step chunkToS3Step) {

        return new JobBuilder("chunkToS3Job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(chunkToS3Step)
                .build();
    }

    @Bean
    public ItemWriter<TargetRecord> s3ChunkItemWriter(
            S3Client s3Client,
            @Value("${aws.s3.bucket-name}") String bucket,
            @Value("${aws.s3.chunk-output-prefix}") String chunkOutputPrefix) {

        return new S3ChunkItemWriter(s3Client, bucket, chunkOutputPrefix);
    }

    @Bean
    public Step chunkToS3Step(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemStreamReader<SourceRecord> sourceRecordItemReader,
            ItemProcessor<SourceRecord, TargetRecord>
                    recordUpperCaseProcessor,
            ItemWriter<TargetRecord> s3ChunkItemWriter) {

        return new StepBuilder("chunkToS3Step", jobRepository)
                .<SourceRecord, TargetRecord>chunk(CHUNK_SIZE, transactionManager)
                .reader(sourceRecordItemReader)
                .processor(recordUpperCaseProcessor)
                .writer(s3ChunkItemWriter)
                .build();
    }

    // ---------- Worker side: processes chunks produced by chunkToDiskJob ----------
    // (Simulates the "ECS worker node" stage picking up chunks and
    // writing results to the target DB.)

    @Bean
    public Job chunkWorkerJob(
            JobRepository jobRepository,
            Step chunkWorkerStep) {

        return new JobBuilder("chunkWorkerJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(chunkWorkerStep)
                .build();
    }

    @Bean
    public Step chunkWorkerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            S3Client s3Client,
            DataSource dataSource,
            @Value("${aws.s3.bucket-name}") String bucket,
            @Value("${aws.s3.chunk-output-prefix}") String chunkOutputPrefix) {

        return new StepBuilder("chunkWorkerStep", jobRepository)
                .tasklet(
                        new S3ChunkWorkerTasklet(s3Client, bucket, chunkOutputPrefix, dataSource),
                        transactionManager)
                .build();
    }

    // ---------- Pattern 7: chunk to Kinesis, consumer processes stream ----------

    @Bean
    public Job chunkToKinesisJob(
            JobRepository jobRepository,
            Step chunkToKinesisStep) {

        return new JobBuilder("chunkToKinesisJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(chunkToKinesisStep)
                .build();
    }

    @Bean
    public ItemWriter<TargetRecord> kinesisChunkItemWriter(
            KinesisClient kinesisClient,
            @Value("${aws.kinesis.stream-name}") String streamName) {

        return new KinesisChunkItemWriter(kinesisClient, streamName);
    }

    @Bean
    public Step chunkToKinesisStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemStreamReader<SourceRecord> sourceRecordItemReader,
            ItemProcessor<SourceRecord, TargetRecord>
                    recordUpperCaseProcessor,
            ItemWriter<TargetRecord> kinesisChunkItemWriter) {

        return new StepBuilder("chunkToKinesisStep", jobRepository)
                .<SourceRecord, TargetRecord>chunk(CHUNK_SIZE, transactionManager)
                .reader(sourceRecordItemReader)
                .processor(recordUpperCaseProcessor)
                .writer(kinesisChunkItemWriter)
                .build();
    }

    @Bean
    public Job kinesisConsumerJob(
            JobRepository jobRepository,
            Step kinesisConsumerStep) {

        return new JobBuilder("kinesisConsumerJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(kinesisConsumerStep)
                .build();
    }

    @Bean
    public Step kinesisConsumerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            KinesisClient kinesisClient,
            DataSource dataSource,
            @Value("${aws.kinesis.stream-name}") String streamName) {

        return new StepBuilder("kinesisConsumerStep", jobRepository)
                .tasklet(
                        new KinesisConsumerTasklet(kinesisClient, streamName, dataSource),
                        transactionManager)
                .build();
    }
}