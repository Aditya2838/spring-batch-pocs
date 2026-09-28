package com.example.batchdemo.config;

import com.example.batchdemo.domain.SourceRecord;

import com.example.batchdemo.domain.TargetRecord;
import com.example.batchdemo.listener.JobLoggingListener;
import com.example.batchdemo.listener.StepLoggingListener;
//import com.example.batchdemo.partition.IdRangePartitioner;
import com.example.batchdemo.processor.RecordUpperCaseProcessor;
import com.example.batchdemo.reader.SourceRecordItemReader;
import com.example.batchdemo.tasklet.ReportTasklet;
import com.example.batchdemo.tasklet.SourceUpdateTasklet;
import com.example.batchdemo.tasklet.ValidationTasklet;
import com.example.batchdemo.writer.TargetRecordItemWriter;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
//import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.database.JdbcCursorItemReader;
//import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
//import org.springframework.core.task.SimpleAsyncTaskExecutor;
//import org.springframework.core.task.TaskExecutor;
//import org.springframework.core.task.TaskExecutor;
//import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
//import java.sql.SQLException;

@Configuration
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    private static final int CHUNK_SIZE = 5;

  
    @Bean
//Spring Batch Job definition
    public Job recordProcessingJob(JobBuilderFactory jobBuilderFactory,
                                    Step validationStep,
                                    Step processingStep,
                                    Step sourceUpdateStep,
                                    Step reportStep,
                                    JobLoggingListener jobLoggingListener) {
        return jobBuilderFactory.get("recordProcessingJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobLoggingListener)
                .start(validationStep)
                .next(processingStep)
                .next(sourceUpdateStep)
                .next(reportStep)
                .build();
    }
//    @Bean
//    public TaskExecutor taskExecutor() {
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//     
//        executor.setCorePoolSize(4);
//        executor.setMaxPoolSize(4);
//        executor.setQueueCapacity(0);
//        executor.setThreadNamePrefix("batch-worker-");
//     
//        executor.initialize();
//        return executor;
//    }

    @Bean
    public JobLoggingListener jobLoggingListener() {
        return new JobLoggingListener();
    }

    @Bean
    public StepLoggingListener stepLoggingListener() {
        return new StepLoggingListener();
    }

    @Bean
    public Step validationStep(StepBuilderFactory stepBuilderFactory,
                                DataSource dataSource) {
        return stepBuilderFactory.get("validationStep")
                .tasklet(new ValidationTasklet(dataSource))
                .build();
    }

//  public JdbcPagingItemReader<SourceRecord> sourceRecordItemReader(
//  DataSource dataSource,
//  @Value("#{jobParameters['batchDate']}") String batchDate) {
//
//log.info("Building SourceRecordItemReader for batchDate={}", batchDate);
//
//return SourceRecordItemReader.build(dataSource);
//} 
    
    @org.springframework.batch.core.configuration.annotation.StepScope
    @Bean
    public JdbcCursorItemReader<SourceRecord> sourceRecordItemReader(
            DataSource dataSource,
            @Value("#{jobParameters['batchDate']}") String batchDate) {
        log.info("Building sourceRecordItemReader for batchDate={}", batchDate="04-09-2026");
        return SourceRecordItemReader.build(dataSource);
    }

    @Bean
    public RecordUpperCaseProcessor recordUpperCaseProcessor() {
        return new RecordUpperCaseProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<TargetRecord> targetRecordItemWriter(DataSource dataSource) {
        return TargetRecordItemWriter.build(dataSource);
    
    }
    
    @Bean
    public Step processingStep(StepBuilderFactory stepBuilderFactory,
                                JdbcCursorItemReader<SourceRecord> sourceRecordItemReader,
                                RecordUpperCaseProcessor recordUpperCaseProcessor,
                                JdbcBatchItemWriter<TargetRecord> targetRecordItemWriter,
                                StepLoggingListener stepLoggingListener) {
        return stepBuilderFactory.get("processingStep")
                .<SourceRecord, TargetRecord>chunk(CHUNK_SIZE)
                .reader(sourceRecordItemReader)
                .processor(recordUpperCaseProcessor)
                .writer(targetRecordItemWriter)
//                .taskExecutor(taskExecutor())
//                .throttleLimit(4)
                
//                .faultTolerant()
//                .skip(Exception.class)
//                .skipLimit(10)
//                .retry(SQLException.class)
//                .retryLimit(3)
                .listener(stepLoggingListener)
                .build();
    }

  
    @Bean
    public Step sourceUpdateStep(StepBuilderFactory stepBuilderFactory,
                                  DataSource dataSource) {
        return stepBuilderFactory.get("sourceUpdateStep")
                .tasklet(new SourceUpdateTasklet(dataSource))
                .build();
    }

    @Bean
    public Step reportStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("reportStep")
                .tasklet(new ReportTasklet())
                .build();
    }

    

//    @Bean
//    public IdRangePartitioner idRangePartitioner() {
//        return new IdRangePartitioner();
//    }
//
//
//    @Bean
//    public TaskExecutor partitionTaskExecutor() {
//        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("partition-worker-");
//        executor.setConcurrencyLimit(4);
//        return executor;
//    }
//
//    @Bean
//    public Step partitionWorkerStep(StepBuilderFactory stepBuilderFactory,
//                                     JdbcCursorItemReader<SourceRecord> sourceRecordItemReader,
//                                     RecordUpperCaseProcessor recordUpperCaseProcessor,
//                                     JdbcBatchItemWriter<TargetRecord> targetRecordItemWriter) {
//        return stepBuilderFactory.get("partitionWorkerStep")
//                .<SourceRecord, TargetRecord>chunk(CHUNK_SIZE)
//                .reader(sourceRecordItemReader)
//                .processor(recordUpperCaseProcessor)
//                .writer(targetRecordItemWriter)
//                .build();
//    }
//
//    @Bean
//    public Step partitionedProcessingStep(StepBuilderFactory stepBuilderFactory,
//                                           Step partitionWorkerStep,
//                                           IdRangePartitioner idRangePartitioner,
//                                           TaskExecutor partitionTaskExecutor) {
//        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
//        partitionHandler.setGridSize(4);
//        partitionHandler.setTaskExecutor(partitionTaskExecutor);
//        partitionHandler.setStep(partitionWorkerStep);
//
//        return stepBuilderFactory.get("partitionedProcessingStep")
//                .partitioner("partitionWorkerStep", idRangePartitioner)
//                .partitionHandler(partitionHandler)
//                .build();
//    }
    
}
