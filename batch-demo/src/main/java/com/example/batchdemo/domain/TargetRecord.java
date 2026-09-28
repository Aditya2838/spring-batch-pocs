package com.example.batchdemo.domain;

import java.time.LocalDateTime;

public class TargetRecord {

    private Integer sourceId;
    private String name;
    private String processedValue;
    private String status;
    private LocalDateTime processedAt;

    public TargetRecord() {
    }

    public TargetRecord(Integer sourceId, String name, String processedValue, String status) {
        this.sourceId = sourceId;
        this.name = name;
        this.processedValue = processedValue;
        this.status = status;
        this.processedAt = LocalDateTime.now();
    }

    public Integer getSourceId() {
        return sourceId;
    }

    public void setSourceId(Integer sourceId) {
        this.sourceId = sourceId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProcessedValue() {
        return processedValue;
    }

    public void setProcessedValue(String processedValue) {
        this.processedValue = processedValue;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }
}
