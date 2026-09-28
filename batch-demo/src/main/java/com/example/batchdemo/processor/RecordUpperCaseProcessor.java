package com.example.batchdemo.processor;

import com.example.batchdemo.domain.SourceRecord;
import com.example.batchdemo.domain.TargetRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class RecordUpperCaseProcessor implements ItemProcessor<SourceRecord, TargetRecord> {

    private static final Logger log = LoggerFactory.getLogger(RecordUpperCaseProcessor.class);

    @Override
    public TargetRecord process(SourceRecord item) {
        String upperValue = item.getValue() == null ? null : item.getValue().toUpperCase();
        log.debug("Processing id={} value='{}' -> '{}'", item.getId(), item.getValue(), upperValue);
        return new TargetRecord(item.getId(), item.getName(), upperValue, "PROCESSED");
    }
}
