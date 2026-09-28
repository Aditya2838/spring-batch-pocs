package com.example.batchdemo.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobLauncherController {

    private final JobLauncher jobLauncher;
    private final Job recordProcessingRestJob;
    private final JobExplorer jobExplorer;

    @Autowired
    public JobLauncherController(
            JobLauncher jobLauncher,
            @Qualifier("recordProcessingRestJob") Job recordProcessingRestJob,
            JobExplorer jobExplorer) {

        this.jobLauncher = jobLauncher;
        this.recordProcessingRestJob = recordProcessingRestJob;
        this.jobExplorer = jobExplorer;
    }

    // Triggers the S3-read -> process -> write pipeline,
    // writing results into target_record_rest.
    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> runJob() {

        Map<String, Object> body = new HashMap<>();

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startAt", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(
                    recordProcessingRestJob,
                    jobParameters
            );

            body.put("jobExecutionId", execution.getId());
            body.put("status", execution.getStatus().toString());
            return ResponseEntity.ok(body);

        } catch (Exception e) {
            body.put("error", e.getMessage());
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(body);
        }
    }

    @GetMapping("/{jobExecutionId}/status")
    public ResponseEntity<Map<String, Object>> getStatus(
            @PathVariable Long jobExecutionId) {

        JobExecution execution = jobExplorer.getJobExecution(jobExecutionId);

        if (execution == null) {
            return ResponseEntity.notFound().build();
        }

        Map<String, Object> body = new HashMap<>();
        body.put("jobExecutionId", execution.getId());
        body.put("status", execution.getStatus().toString());
        body.put("startTime", execution.getStartTime());
        body.put("endTime", execution.getEndTime());
        body.put("exitStatus", execution.getExitStatus().getExitCode());

        return ResponseEntity.ok(body);
    }
}