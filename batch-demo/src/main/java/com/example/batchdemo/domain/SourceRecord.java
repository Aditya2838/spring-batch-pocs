package com.example.batchdemo.domain;

import java.time.LocalDateTime;

public class SourceRecord {

    private Integer id;
    private String name;
    private String value;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public SourceRecord() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "SourceRecord{id=" + id + ", name='" + name + "', value='" + value
                + "', status='" + status + "'}";
    }
}
