package gobblin.elasticsearch.writer;

public class ElasticsearchWriterBulkResponseException extends RuntimeException {

  public ElasticsearchWriterBulkResponseException(String message) {
    super(message);
  }
}
