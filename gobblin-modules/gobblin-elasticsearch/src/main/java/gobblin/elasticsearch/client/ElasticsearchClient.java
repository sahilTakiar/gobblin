package gobblin.elasticsearch.client;

public interface ElasticsearchClient {

  void addRecord(Record record);

  void flush();
}
