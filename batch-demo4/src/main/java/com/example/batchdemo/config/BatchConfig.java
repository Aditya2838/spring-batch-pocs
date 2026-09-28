package com.example.batchdemo.config;

import com.example.batchdemo.domain.SourceRecord;
import com.example.batchdemo.domain.TargetRecord;
import com.example.batchdemo.processor.RecordUpperCaseProcessor;
import com.example.batchdemo.reader.S3CsvItemReader;
import com.example.batchdemo.writer.TargetRecordItemWriter;

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
}