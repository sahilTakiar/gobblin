package gobblin.writer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonWriter;

import gobblin.configuration.State;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.atomic.AtomicLong;


public class JsonHdfsDataWriter extends FsDataWriter<JsonElement> {

  private static final Gson GSON = new Gson();

  private final JsonWriter jsonWriter;
  protected final AtomicLong count = new AtomicLong(0);

  public JsonHdfsDataWriter(FsDataWriterBuilder<?, JsonElement> builder, State properties) throws IOException {
    super(builder, properties);
    this.jsonWriter = GSON.newJsonWriter(new BufferedWriter(new OutputStreamWriter(createStagingFileOutputStream())));
  }

  //  @VisibleForTesting
  //  JsonHdfsDataWriter(FsDataWriterBuilder<?, JsonElement> builder, State properties, JsonWriter jsonWriter) {
  //    super(builder, properties);
  //    this.jsonWriter = jsonWriter;
  //  }

  @Override
  public void write(JsonElement record) throws IOException {
    GSON.toJson(record, this.jsonWriter);
    this.count.incrementAndGet();
  }

  @Override
  public long recordsWritten() {
    return this.count.get();
  }
}
