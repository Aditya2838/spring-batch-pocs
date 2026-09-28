package com.example.batchdemo.writer;

import com.example.batchdemo.domain.TargetRecord;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import javax.sql.DataSource;

public final class TargetRecordItemWriter {

    private TargetRecordItemWriter() {
    }

    public static JdbcBatchItemWriter<TargetRecord> build(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<TargetRecord>()
                .dataSource(dataSource)
                .sql("INSERT INTO target_records (source_id, name, processed_value, status, processed_at) "
                        + "VALUES (:sourceId, :name, :processedValue, :status, :processedAt)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .assertUpdates(true)
                .build();
    }
}
