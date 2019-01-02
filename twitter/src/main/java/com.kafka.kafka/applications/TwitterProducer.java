package kafka.applications;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private String consumerKey = "WwVSHd1mKg2GS5LHlkzlY6fx6";
  private String consumerSecret = "oybu2lbQfzqIE1WQbAZetYPmKfGs9o9g9nnFOoYVwYDxv8b5SO";
  private String token = "1078133566632947717-OCuwOAoFnOp06AbrTn8Gb0lj4Tk0Lq";
  private String secret = "cqt2Eb9tzVd5yK1Xla7ZJNpj1H6kWKX9oVOcLLFZIhkm9";
  private Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

  public TwitterProducer() {
  }

  public static void main(String[] args) {
    String topic = args[0];
    new TwitterProducer().run(topic);
  }

  private void run(String topic) {
    log.info("<<< Setting up");
    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
    /**
     * Create a twitter client.
     */
    Client client = createTwitterClient(msgQueue);
    client.connect();

    /**
     * Create kafka producer
     */
    KafkaProducer producer = createKafkaProducer();

    /**
     * Graceful shutdown/exit
     */
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("<<< Stopping application");
      log.info("<<< Shutting down client from Twitter");
      client.stop();
      log.info("<<< Closing producer");
      producer.close();
      log.info("<<< Done!");
    }));

    /**
     * loop to send tweets to kafka
     * on a different thread, or multiple different threads....
     */
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        log.info("<<< Tweet: " + msg);
        producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null) {
              System.err.println("<<< Error sending to kafka topic: " + e);
            }
          }
        });
      }
    }
    log.info("<<< End of application!");
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {
    Client hosebirdClient = null;
    /**
     * Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
     */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    /**
     * Set up some followings and track terms. We follow terms for this example.
     */
    List<String> terms = Lists.newArrayList("trump", "china");
    hosebirdEndpoint.trackTerms(terms);

    /**
     * These secrets should be read from a config file
     */
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01") // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue)); // optional: use this if you want to process client events

    hosebirdClient = builder.build();
    return hosebirdClient;
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    final String bootstrapServer = "localhost:9092";
    /**
     * Create producer configurations
     */
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    /**
     * Configure a safer producer
     */
    producerConfig.put( ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true" );
    producerConfig.put( ProducerConfig.ACKS_CONFIG, "all" );
    producerConfig.put( ProducerConfig.RETRIES_CONFIG, Integer.toString( Integer.MAX_VALUE ) );
    producerConfig.put( ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5" );
    /**
     * Configure a high throughput producer (at the expense of a bit of latency and CPU usage)
     */
    producerConfig.put( ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy" ); // snappy compression made by Google
    producerConfig.put( ProducerConfig.LINGER_MS_CONFIG, "1000" );  // 20 milliseconds
    producerConfig.put( ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString( 32 * 1024 ) ); // 32 kilobytes batch size
    /**
     * Create the producer
     */
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
    return producer;
  }
}
