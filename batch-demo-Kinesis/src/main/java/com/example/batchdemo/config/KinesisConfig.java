package com.example.batchdemo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
public class KinesisConfig {

    @Bean
    public KinesisClient kinesisClient(@Value("${aws.region}") String region) {
        return KinesisClient.builder()
                .region(Region.of(region))
                .build();
    }
}