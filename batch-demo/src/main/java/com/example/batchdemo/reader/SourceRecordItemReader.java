package com.example.batchdemo.reader;

import com.example.batchdemo.domain.SourceRecord;

import com.example.batchdemo.mapper.SourceRecordRowMapper;
import org.springframework.batch.item.database.JdbcCursorItemReader;
//import org.springframework.batch.item.database.JdbcPagingItemReader;
//import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
//import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
//import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
//
//import java.util.HashMap;
//import java.util.Map;

import javax.sql.DataSource;

public final class SourceRecordItemReader {

    private SourceRecordItemReader() {
    }

    public static JdbcCursorItemReader<SourceRecord> build(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<SourceRecord>()
                .name("sourceRecordItemReader")
                .dataSource(dataSource)
                .sql("SELECT * FROM source_records WHERE status = 'NEW'")
                .rowMapper(new SourceRecordRowMapper())
                .build();
  }
    
    public static JdbcCursorItemReader<SourceRecord> buildForRange(DataSource dataSource, int minId, int maxId) {
        return new JdbcCursorItemReaderBuilder<SourceRecord>()
                .name("partitionSourceRecordItemReader")
                .dataSource(dataSource)
                .sql("SELECT * FROM source_records WHERE status = 'NEW' AND id BETWEEN " + minId + " AND " + maxId)
                .rowMapper(new SourceRecordRowMapper())
                .build();
    }
    
//    public static JdbcPagingItemReader<SourceRecord> build(DataSource dataSource) {
//    	 
//        MySqlPagingQueryProvider queryProvider =
//                new MySqlPagingQueryProvider();
// 
//        queryProvider.setSelectClause(
//                "SELECT id, name, value, status, created_at, updated_at");
// 
//        queryProvider.setFromClause(
//                "FROM source_records");
// 
//        queryProvider.setWhereClause(
//                "WHERE status = 'NEW'");
// 
//        Map<String, Order> sortKeys = new HashMap<>();
//        sortKeys.put("id", Order.ASCENDING);
// 
//        queryProvider.setSortKeys(sortKeys);
// 
//        return new JdbcPagingItemReaderBuilder<SourceRecord>()
//                .name("sourceRecordItemReader")
//                .dataSource(dataSource)
//                .queryProvider(queryProvider)
//                .rowMapper(new SourceRecordRowMapper())
//                .pageSize(100)
//                .fetchSize(100)
//                .saveState(false)
//                .build();
//    }

    

}
