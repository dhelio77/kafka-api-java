package com.kafka.kafka.applications;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerP4 {

  private static String hostname = "kafka-api-6888318099.ap-southeast-2.bonsaisearch.net";
  private static String username = "bv2n8abloj";
  private static String password = "36tt77rij9";
  private static Logger log = LoggerFactory.getLogger(ElasticSearchConsumerP4.class.getName());
  private static String bootstrapServers = "localhost:9092";
  private static String AUTO_OFFSET_RESET_CONFIG_VALUE = "earliest";

  private static IndexResponse indexResponse;
  private static IndexRequest indexRequest;
  private static JsonParser jsonParser;

  private static BulkResponse bulkItemResponses;
  private static BulkRequest bulkRequest;

  /**
   * P1.Creates a message and send the message to Elastic Search.
   * https://app.bonsai.io/clusters/kafka-api-6888318099/console
   * Test if PUT is successful, use GET using /twitter/tweets/<id>
   *
   * P2.Creates an ID to make the consumer idempotent
   *
   * P3.Implement recommended consumer configuration.
   * enable.auto.commit=true
   * synchronous processing or commit after max poll (max.poll.records)
   *
   * P4.Performance improvement using elastic search batching
   * e.g. BulkRequest and BulkResponse
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    String topic = args[0];
    String groupId = args[1];
    int recordCount = 0;
    RestHighLevelClient restHighLevelClient = createClient();
    /**
     * Creates consumer
     */
    KafkaConsumer<String, String> kafkaConsumer = createConsumer(topic, groupId);

    while (true) {
      ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
      recordCount = consumerRecords.count();
      log.info("<<< Received: " + recordCount + " records!");

      /**
       * Bulk Request
       */
      bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        /**
         * This value will be passed on to the elastic search
         */
        String record = consumerRecord.value();

        /**
         * Two ways to make an id to make a consumer idempotent
         * 1. id using kafka topic + partition + offset
         * e.g. consumerRecord.topic() + consumerRecord.partition() + consumerRecord.offset()
         * 2. id using tweeter's id_str
         */
        String id = extractIdFromTweet(record);
        /**
         * Creates index request and pass the json string
         */
        indexRequest = new IndexRequest(
            "tweeter",
            "tweets",
            id  // this will make the consumer idempotent
        ).source(record, XContentType.JSON);
        /**
         * Add bulk request.
         * Note: IndexResponse is replaced by BulkResponse.
         */
        bulkRequest.add(indexRequest);
      }
      if (recordCount > 0) {
        /**
         * Bulk response
         */
        try {
          bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
          log.warn("<<< Skipping record: " + bulkRequest.getDescription());
        }
        log.info("<<< Committing offsets...");
        /**
         * Manual commit happens here after the for loop.
         */
        kafkaConsumer.commitSync();
        log.info("<<< Offsets have been committed");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Extract id_str of twitter tweet using gson
   *
   * @param record
   */
  private static String extractIdFromTweet(String record) {
    jsonParser = new JsonParser();
    return jsonParser.parse(record)
        .getAsJsonObject()
        .get("id_str")
        .getAsString();
  }

  private static KafkaConsumer<String, String> createConsumer(String topic, String groupId) {

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG_VALUE);

    /**
     * Config for P3. Consumer Offset Commit Strategy.
     * Manual commit of offsets.
     */
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // disable auto commit of offsets
    consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // maximum records to poll before commit

    KafkaConsumer kafkaConsumer = new KafkaConsumer(consumerConfig);
    kafkaConsumer.subscribe(Arrays.asList(topic));
    return kafkaConsumer;
  }

  private static RestHighLevelClient createClient() {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder restClientBuilder = RestClient.builder(
        new HttpHost(
            hostname,
            443,
            "https"))
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
