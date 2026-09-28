package com.example.batchdemo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.Duration;
import java.util.Date;


public class JobLoggingListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobLoggingListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Job [{}] starting. Start Time = {}",
                jobExecution.getJobInstance().getJobName(), new Date());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Date start = jobExecution.getStartTime();
        Date end = jobExecution.getEndTime() != null ? jobExecution.getEndTime() : new Date();
        Duration duration = start != null ? Duration.ofMillis(end.getTime() - start.getTime()) : Duration.ZERO;

        log.info("Job [{}] finished. Start Time = {}, End Time = {}, Duration = {}ms, Final Status = {}",
                jobExecution.getJobInstance().getJobName(), start, end,
                duration.toMillis(), jobExecution.getStatus());
    }
}
