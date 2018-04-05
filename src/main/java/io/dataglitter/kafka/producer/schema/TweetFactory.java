package io.dataglitter.kafka.producer.schema;

import io.dataglitter.kafka.commons.generics.SchemaAdaptor;
import io.dataglitter.kafka.producer.constant.TweetConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Created by reddys on 10/03/2018.
 */
@Service
public class TweetFactory extends TweetAbstractFactory {

    @Autowired
    @Qualifier("tweetV1")
    private SchemaAdaptor tweetV1;

    @Autowired
    @Qualifier("tweetV2")
    private SchemaAdaptor tweetV2;

    @Override
    public SchemaAdaptor getSchemaAdaptor(String schemaName) {
        if (schemaName.equalsIgnoreCase(TweetConstants.TWEET_V1)) {
            return tweetV1;
        } else if (schemaName.equalsIgnoreCase(TweetConstants.TWEET_V2)) {
            return tweetV2;
        }
        return null;
    }

}
