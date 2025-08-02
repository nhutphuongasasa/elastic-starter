package com.example.elasticsearch_spring_starter.config;

import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
// import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
// import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration; // Lớp cơ sở để cấu hình Elasticsearch trong Spring Data
import org.springframework.context.annotation.Configuration;

import com.example.elasticsearch_spring_starter.properties.RestClientPoolProperties;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@Configuration
@EnableConfigurationProperties(RestClientPoolProperties.class)
public class RestAutoConfigure {
    private final static String SCHEME = "http";
    private final static String URI_SPLIT = ":";

    @Autowired
    private ElasticsearchProperties restProperties;

    @Autowired
    private RestClientPoolProperties poolProperties;

    @Bean
    public ElasticsearchClient elasticsearchClient(){
        List<String> urlArr = restProperties.getUris();
        HttpHost[] httpPostArr = new HttpHost[urlArr.size()];

        for(int i = 0; i < urlArr.size(); i++){
            HttpHost httpHost = new HttpHost(urlArr.get(i).split(URI_SPLIT)[0].trim(),
                Integer.parseInt(urlArr.get(i).split(URI_SPLIT)[1].trim()),
                SCHEME
            );
            httpPostArr[i] = httpHost;
        }

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY, 
            new UsernamePasswordCredentials(
                restProperties.getUsername(),
                restProperties.getPassword()
            )
        );

        RestClientBuilder builder = RestClient.builder(httpPostArr)
            .setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    .setMaxConnTotal(poolProperties.getMaxConnectPerRoute())
                    .setMaxConnPerRoute(poolProperties.getMaxConnectPerRoute());
                
                return httpClientBuilder;
            })
            .setRequestConfigCallback(requestConfigBuilder -> {
                requestConfigBuilder.setConnectTimeout(poolProperties.getConnectTimeOut())
                    .setSocketTimeout(poolProperties.getSocketTimeOut())
                    .setConnectionRequestTimeout(poolProperties.getConnectionRequestTimeOut());
                
                return requestConfigBuilder;
            });

        RestClient restClient = builder.build();
        RestClientTransport transport = new RestClientTransport(
            restClient,
            new JacksonJsonpMapper()
        );

        return new ElasticsearchClient(transport);
    }
}
