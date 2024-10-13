package com.epam.krystseu.java_low_level_rest_client.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class CustomElasticsearchClient  {

    private final RestClient restClient;

    public CustomElasticsearchClient(@Value("${elasticsearch.host}") String elasticsearchHost) {
        this.restClient = RestClient.builder(HttpHost.create(elasticsearchHost)).build();
    }

    public Response performRequest(Request request) throws IOException {
        return restClient.performRequest(request);
    }

    public void close() throws IOException {
        restClient.close();
    }
}