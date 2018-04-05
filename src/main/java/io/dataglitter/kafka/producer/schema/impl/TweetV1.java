package io.dataglitter.kafka.producer.schema.impl;

import io.dataglitter.kafka.commons.generics.SchemaAdaptor;
import io.dataglitter.kafka.producer.constant.TweetConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import twitter4j.Status;

import java.io.InputStream;

/**
 * Created by reddys on 10/03/2018.
 */
@Component
public class TweetV1 implements SchemaAdaptor<Schema, Status, GenericRecord> {

    private Resource tweetSchema;

    private Schema schema;

    private final int VERSION = 1;

    public TweetV1(@Value("classpath:schema/tweet-v1.avsc") Resource tweetSchema) throws Exception {
        this.tweetSchema = tweetSchema;
        this.schema = this.adapt();
    }

    @Override
    public Schema adapt() throws Exception {
        Schema adaptedSchema;
        try (InputStream is = this.tweetSchema.getInputStream()) {
            adaptedSchema = new Schema.Parser().parse(is);
        }
        return adaptedSchema;
    }

    @Override
    public GenericRecord populateRecord(Status status) {
        GenericRecord tweet;
        tweet = new GenericData.Record(this.schema);
        tweet.put(TweetConstants.VERSION, VERSION);
        tweet.put(TweetConstants.ID, status.getId());
        tweet.put(TweetConstants.TEXT, status.getText());
        tweet.put(TweetConstants.LANG, status.getLang());
        tweet.put(TweetConstants.IS_RETWEET, status.isRetweet());
        return tweet;
    }

}
