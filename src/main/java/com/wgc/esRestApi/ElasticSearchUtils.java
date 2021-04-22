package com.wgc.esRestApi;

/**
 * elastic search 6.5.1 rest api
 */

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ElasticSearchUtils {
    //x-pack账号
    private static final String USER = "wgc_test";
    //x-pack密码
    private static final String PASSWORD = "wRgroX35";
    // es集群http地址
    private static final List<String> ELASTIC_URL = Arrays.asList("134.64.14.37:9201","134.64.14.38:9201");
    //实例化一个RestHighLevelClient对象
    //volatile 是一个类型修饰符。volatile 的作用是作为指令关键字，确保本条指令不会因编译器的优化而省略。
    private static volatile RestHighLevelClient ELASTIC_UTIL = null;
    private static final Object LOCK = new Object();

    //实例化一个RestHighLevelClient对象，单例模式，懒汉式4，双重校验锁
    public static RestHighLevelClient getElasticInstance() {
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

    //初始化
    private static RestHighLevelClient init() {
        HttpHost[] httpHosts = new HttpHost[ELASTIC_URL.size()];
        System.out.println("httpHosts: " + httpHosts);
        for (int i = 0; i < ELASTIC_URL.size(); i++) {
            httpHosts[i] = HttpHost.create(ELASTIC_URL.get(i));
            System.out.println("httpHosts[" + i + "]:" + httpHosts[i]);
        }
        //配置RestClient
        //这是low-reset的api方法
        //https://artifacts.elastic.co/javadoc/org/elasticsearch/client/elasticsearch-rest-client/6.5.4/allclasses.html
        //org.apache.http.impl.nio.client.HttpAsyncClientBuilder customizeHttpClient​(org.apache.http.impl.nio.client.HttpAsyncClientBuilder httpClientBuilder)
        //Allows to customize the CloseableHttpAsyncClient being created and used by the RestClient.
        // Commonly used to customize the default CredentialsProvider for authentication or the SchemeIOSessionStrategy for communication through ssl without losing any other useful default value that the RestClientBuilder internally sets, like connection pooling.
        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        //禁用验证缓存
                        httpAsyncClientBuilder.disableAuthCaching();
                        //设置验证方式及账号
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        //账号和密码
                        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USER,PASSWORD));
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        return httpAsyncClientBuilder;
                    }
                });

        //初始化restHighClien连接
        final RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        //注册关闭连接构
        //jvm中增加一个关闭的钩子，当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子，当系统执行完这些钩子后，jvm才会关闭。
        // 所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁等操作。
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                try {
                    restHighLevelClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
        return restHighLevelClient;
    }

    //主函数
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ElasticSearchUtils.getElasticInstance();
        /**
         * 1、创建索引
         */
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("wgc_test_index_2019-12-04");
        //索引设置
        request.settings(Settings.builder()
                .put("index.number_of_shards", 5) //主分片
                .put("index.number_of_replicas", 1) //副本数
                .put("index.routing.allocation.total_shards_per_node",1) //每个节点上的分片包括副本分片
        );
        //类型映射
        //需要的是一个JSON字符串
        request.mapping("tweet",
                "  {\n" +
                        "    \"tweet\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"message\": {\n" +
                        "          \"type\": \"text\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }", XContentType.JSON);

        //同步执行
        CreateIndexResponse createIndexResponse = client.indices().create(request,RequestOptions.DEFAULT);
        System.out.println("---索引创建成功.");

        /**
         * 创建或者插入
         */
        IndexRequest indexRequest = new IndexRequest("wgc_test_index_2019-12-12", "doc", "3");
        String jsonString = "{" +
                "\"user\":\"wangtao\"," +
                "\"postDate\":\"2019-12-02\"," +
                "\"message\":\"trying out Elasticsearch is good\"" +
                "}";
        indexRequest.source(jsonString, XContentType.JSON);
        //同步执行
        IndexResponse indexResponse = client.index(indexRequest,RequestOptions.DEFAULT );
        System.out.println(indexResponse.toString() + "---插入成功.");

        /**
         * 检索
         */
        GetRequest getRequest = new GetRequest("wgc_test_index_2019-12-12", "doc", "3");
        //使用Get API按ID检索文档,getRequest - the request,options - the request options (e.g. headers), use RequestOptions.DEFAULT if nothing needs to be customized
        try {
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            System.out.println(getResponse.toString() + "---检索完成.");
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("---索引不存在.");
            }
        }
    }
}

