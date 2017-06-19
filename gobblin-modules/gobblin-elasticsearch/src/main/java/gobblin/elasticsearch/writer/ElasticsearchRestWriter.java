package gobblin.elasticsearch.writer;

import gobblin.writer.Batch;
import gobblin.writer.BatchAsyncDataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.Future;

// TODO
public class ElasticsearchRestWriter implements BatchAsyncDataWriter<byte[], BulkResponse> {

  @Override
  public Future<WriteResponse<BulkResponse>> write(Batch<byte[]> batch, @Nullable WriteCallback callback) {
    return null;
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}
