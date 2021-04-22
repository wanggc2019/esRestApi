package com.wgc.esRestApi;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import sun.java2d.DisposerRecord;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class ElasticResetClient implements DisposerRecord {
    private static RestHighLevelClient restHighLevelClient;
    private static final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    private static final Logger logger = LogManager.getLogger(ElasticResetClient.class.getName());
    private static final Object LOCK = new Object();
    private static volatile RestHighLevelClient ELASTIC_UTIL = null;

    private static RestHighLevelClient getRestHighLevelClient() {
        if (ELASTIC_UTIL != null) {
            return ELASTIC_UTIL;
        }
        synchronized (LOCK) {
            //二次检查
            if (ELASTIC_UTIL != null) {
                return ELASTIC_UTIL;
            }
            ELASTIC_UTIL = init();
            return ELASTIC_UTIL;
        }
    }

    private static RestHighLevelClient init() {
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("wgc_test", "wRgroX35"));
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("134.64.14.37", 9201, "http"),
                        new HttpHost("134.64.14.38",9201,"http")
                ).setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.disableAuthCaching();
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                })
        );

/*        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    restHighLevelClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }}));*/

        logger.info("---初始化完成.");
        return restHighLevelClient;
    }

    @Override
    public void dispose() {
        if (restHighLevelClient != null) {
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        RestHighLevelClient client = ElasticResetClient.getRestHighLevelClient();
        logger.info("ok1");
        /**
         * 添加索引
         */
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user","wgc");
        jsonMap.put("postDate",new Date());
        jsonMap.put("message","I am a hight and rich and handsome");

        IndexRequest request = new IndexRequest("wgc_test_20191212","doc","1").source(jsonMap);
        logger.info("ok2");
        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            logger.info("ok3");
            logger.info(response.toString() + "---索引添加完成.");
        } catch (IOException  e) {
            e.printStackTrace();
        }

        /**
         * 检索
         */
        try {
            GetRequest getRequest = new GetRequest("wgc_test_20191212","doc","1");
            GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
            logger.info(getResponse.toString() + "---检索完成.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
