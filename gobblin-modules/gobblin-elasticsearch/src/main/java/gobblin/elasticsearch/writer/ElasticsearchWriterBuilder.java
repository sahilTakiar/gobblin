package gobblin.elasticsearch.writer;

import com.google.gson.JsonObject;
import com.typesafe.config.Config;
import gobblin.configuration.State;
import gobblin.elasticsearch.ElasticsearchWriterConfigurationKeys;
import gobblin.util.ConfigUtils;
import gobblin.writer.AsyncWriterManager;
import gobblin.writer.BufferedAsyncDataWriter;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.SequentialBasedBatchAccumulator;

import java.io.IOException;
import java.util.Properties;

public class ElasticsearchWriterBuilder extends DataWriterBuilder {

  @Override
  public DataWriter build() throws IOException {

    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config config = ConfigUtils.propertiesToConfig(taskProps);

    SequentialBasedBatchAccumulator<JsonObject> batchAccumulator = new SequentialBasedBatchAccumulator<>(taskProps);

//    switch (ElasticsearchWriterConfigurationKeys.ClientType.valueOf(ConfigUtils.getString(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_TYPE))) {
//      case REST:
//      case TRANSPORT:
//    }
    ElasticsearchTransportClientWriter elasticsearchWriter = new ElasticsearchTransportClientWriter(config);
    // TODO fix the types
    BufferedAsyncDataWriter bufferedAsyncDataWriter = new BufferedAsyncDataWriter(batchAccumulator, elasticsearchWriter);

    return AsyncWriterManager.builder()
        .failureAllowanceRatio(0.0)
        .retriesEnabled(false)
        .config(config)
        .asyncDataWriter(bufferedAsyncDataWriter)
        .build();
  }
}
