package gobblin.elasticsearch.writer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import gobblin.elasticsearch.ElasticsearchWriterConfigurationKeys;
import gobblin.util.ConfigUtils;
import gobblin.writer.Batch;
import gobblin.writer.BatchAsyncDataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


@Slf4j
class ElasticsearchTransportClientWriter implements BatchAsyncDataWriter<JsonObject, BulkResponse> {

  private final Client client;
  private final String indexName;
  private final String indexType;

  ElasticsearchTransportClientWriter(Config config) throws UnknownHostException {
    this.client = createTransportClient(config);
    this.indexName = config.getString(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_NAME);
    this.indexType = config.getString(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_TYPE);
  }

  @Override
  public Future<WriteResponse<BulkResponse>> write(Batch<JsonObject> batch, @Nullable WriteCallback callback) {
    BulkRequestBuilder bulkRequestBuilder = this.client.prepareBulk();

    for (JsonObject record : batch.getRecords()) {
      bulkRequestBuilder.add(this.client.prepareIndex(this.indexName, this.indexType).setSource(record.toString().getBytes(
              ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));
    }

    // TODO cleanup
    // TODO why is callback null?
    if (callback == null) {
      ListenableActionFuture<BulkResponse> future = bulkRequestBuilder.execute();
      return new Future<WriteResponse<BulkResponse>>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
          return future.isCancelled();
        }

        @Override
        public boolean isDone() {
          return future.isDone();
        }

        @Override
        public WriteResponse<BulkResponse> get() throws InterruptedException, ExecutionException {
          BulkResponse bulkResponse = future.actionGet();
          return new BulkWriteResponse(bulkResponse);
        }

        @Override
        public WriteResponse<BulkResponse> get(long timeout,
                                 TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          BulkResponse bulkResponse = future.actionGet(timeout, unit);
          return new BulkWriteResponse(bulkResponse);
        }
      };
    } else {
      bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
          if (bulkItemResponses.hasFailures()) {
            callback.onFailure(new ElasticsearchWriterBulkResponseException(bulkItemResponses.buildFailureMessage()));
          } else {
            callback.onSuccess(new BulkWriteResponse(bulkItemResponses));
          }
        }

        @Override
        public void onFailure(Throwable e) {
          callback.onFailure(e);
        }
      });
    }

    return null;
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {
    this.client.close();
  }

  private static class BulkWriteResponse implements WriteResponse<BulkResponse> {

    private final BulkResponse bulkResponse;

    public BulkWriteResponse(BulkResponse bulkResponse) {
      this.bulkResponse = bulkResponse;
    }

    @Override
    public BulkResponse getRawResponse() {
      return bulkResponse;
    }

    @Override
    public String getStringResponse() {
      return bulkResponse.toString(); // replace with write()
    }

    @Override
    public long bytesWritten() {
      return -1; // fix this
    }
  }

  @VisibleForTesting
  Client getTransportClient() {
    return this.client;
  }

  // TODO consider moving into a separate class just for creating transport client, if this class gets too big
  private TransportClient createTransportClient(Config config) throws UnknownHostException {
    TransportClient transportClient;

    // Set TransportClient settings
    Settings.Builder settingsBuilder = Settings.builder();
    if (config.hasPath(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SETTINGS)) {
      settingsBuilder.put(ConfigUtils.configToProperties(config,
              ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SETTINGS));
    }
    transportClient = TransportClient.builder().settings(settingsBuilder).build();

    // Add TransportClient host addresses

    // If list is empty, connect to the default host and port
    if (!config.hasPath(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_HOSTS)) {
      addDefaultTransportClientHost(transportClient);
    } else {
      // Get list of hosts
      List<String> hosts = config.getStringList(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_HOSTS);
      addTransportClientHosts(transportClient, hosts);
    }
    return transportClient;
  }

  private void addDefaultTransportClientHost(TransportClient transportClient) throws UnknownHostException {
    log.info("No hosts specified for Elasticsearch writer, connecting to default host {} and port {}",
            ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_DEFAULT_HOST,
            ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_DEFAULT_PORT);
    transportClient.addTransportAddress(new InetSocketTransportAddress(
            InetAddress.getByName(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_DEFAULT_HOST),
            ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_DEFAULT_PORT));
  }

  private void addTransportClientHosts(TransportClient transportClient,
                                       List<String> hosts) throws UnknownHostException {
    // Iterate through all hosts and add each one to the TransportClient
    Splitter hostSplitter = Splitter.on(":").trimResults();
    for (String host : hosts) {

      List<String> hostSplit = hostSplitter.splitToList(host);
      Preconditions.checkArgument(hostSplit.size() == 1 || hostSplit.size() == 2,
              "Malformed host name for Elasticsearch writer: " + host + " host names must be of form [host] or [host]:[port]");

      InetAddress hostInetAddress = InetAddress.getByName(hostSplit.get(0));
      InetSocketTransportAddress hostAddress = null;

      if (hostSplit.size() == 1) {
        hostAddress = new InetSocketTransportAddress(hostInetAddress,
                ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_DEFAULT_PORT);
      } else if (hostSplit.size() == 2) {
        hostAddress = new InetSocketTransportAddress(hostInetAddress, Integer.parseInt(hostSplit.get(1)));
      }

      log.info("Adding host {} to Elasticsearch writer", hostAddress);
      transportClient.addTransportAddress(hostAddress);
    }
  }
}
