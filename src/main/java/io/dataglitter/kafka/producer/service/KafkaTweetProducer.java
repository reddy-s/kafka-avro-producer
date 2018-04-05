package io.dataglitter.kafka.producer.service;

import io.dataglitter.kafka.commons.generics.SchemaAdaptor;
import io.dataglitter.kafka.producer.schema.TweetFactory;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.stereotype.Service;
import twitter4j.*;

/**
 * Created by reddys on 10/03/2018.
 */
@Service
public class KafkaTweetProducer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTweetProducer.class);

    @Value("${twitter.hashtags}")
    private String hashtags[];

    @Value("${kafka.topic}")
    private String topic;

    @Value("${schema.version}")
    private String tweetSchemaVersion;

    @Autowired
    private KafkaTemplate<String, GenericRecord> template;

    @Autowired
    private TweetFactory tweetFactory;

    @Override
    public void run(String... args) throws Exception {
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        SchemaAdaptor tweetAdaptor = tweetFactory.getSchemaAdaptor(tweetSchemaVersion);
        twitterStream.addListener(new StatusAdapter() {
            @Override
            public void onStatus(Status status) {
            template.send(topic, (GenericRecord) tweetAdaptor.populateRecord(status));
            logger.info(Long.toString(status.getId()) + " " + tweetSchemaVersion);
            }
        });
        twitterStream.filter(getFilters());
    }

    public FilterQuery getFilters(){
        FilterQuery filters = new FilterQuery();
        filters.track(hashtags);
        return filters;
    }

}
