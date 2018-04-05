package io.dataglitter.kafka.producer.schema;

import io.dataglitter.kafka.commons.generics.SchemaAdaptor;

/**
 * Created by reddys on 10/03/2018.
 */
public abstract class TweetAbstractFactory {

    abstract SchemaAdaptor getSchemaAdaptor(String schemaName);

}
