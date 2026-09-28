package com.example.batchdemo.mapper;

import com.example.batchdemo.domain.SourceRecord;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


public class SourceRecordRowMapper implements RowMapper<SourceRecord> {

    @Override
    public SourceRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
        SourceRecord record = new SourceRecord();
        record.setId(rs.getInt("id"));
        record.setName(rs.getString("name"));
        record.setValue(rs.getString("value"));
        record.setStatus(rs.getString("status"));

        Timestamp createdAt = rs.getTimestamp("created_at");
        if (createdAt != null) {
            record.setCreatedAt(createdAt.toLocalDateTime());
        }
        Timestamp updatedAt = rs.getTimestamp("updated_at");
        if (updatedAt != null) {
            record.setUpdatedAt(updatedAt.toLocalDateTime());
        }
        return record;
    }
}
