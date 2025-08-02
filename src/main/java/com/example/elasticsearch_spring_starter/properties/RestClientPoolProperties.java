package com.example.elasticsearch_spring_starter.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import lombok.Getter;
import lombok.Setter;

@RefreshScope
@Setter
@Getter
@ConfigurationProperties(prefix = "phuong.elasticsearch.rest-pool")
public class RestClientPoolProperties {
    private Integer connectTimeOut = 1000;

    private Integer socketTimeOut = 30000;

    private Integer connectionRequestTimeOut = 500;

    private Integer maxConnextNum = 30;

    private Integer maxConnectPerRoute = 10;
}
