package kafka.applications;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumerP1 {

  private static String hostname = "kafka-api-6888318099.ap-southeast-2.bonsaisearch.net";
  private static String username = "bv2n8abloj";
  private static String password = "36tt77rij9";
  private static Logger log = LoggerFactory.getLogger(kafka.applications.ElasticSearchConsumerP1.class.getName());

  /**
   * P1.Creates a message and send the message to Elastic Search
   * https://app.bonsai.io/clusters/kafka-api-6888318099/console
   * Test if PUT is successful, use GET using /twitter/tweets/<id>
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    final String topic = args[0];
    RestHighLevelClient restHighLevelClient = createClient();
    IndexResponse indexResponse = null;
    /**
     * Dummy request value
     */
    String jsonString = "{" +
        "\"code\":\"rancheta\"" +
        "}";
    /**
     * Creates index request and pass the json string
     */
    IndexRequest indexRequest = new IndexRequest(
        "twitter",
        "tweets"
    ).source(jsonString, XContentType.JSON);
    indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    /**
     * Id that will be generated and use to verify the value
     */
    String id = indexResponse.getId();
    log.info("<<< ID: " + id);
    /**
     * Close gracefully
     */
    restHighLevelClient.close();
  }

  private static RestHighLevelClient createClient() {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder restClientBuilder = RestClient.builder(
        new HttpHost(hostname, 443, "https"))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }
        });
    RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    return restHighLevelClient;
  }
}
