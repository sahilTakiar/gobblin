package gobblin.elasticsearch;

public class ElasticsearchWriterConfigurationKeys {

  private static final String ELASTICSEARCH_WRITER_PREFIX = "writer.elasticsearch";

  private static String prefix(String value) { return ELASTICSEARCH_WRITER_PREFIX + "." + value;};

  public static final String ELASTICSEARCH_WRITER_SETTINGS = prefix("settings");
  public static final String ELASTICSEARCH_WRITER_HOSTS = prefix("hosts");
  public static final String ELASTICSEARCH_WRITER_INDEX_NAME = prefix("index.name");
  public static final String ELASTICSEARCH_WRITER_INDEX_TYPE = prefix("index.type");
  public static final String ELASTICSEARCH_WRITER_CLIENT_TYPE = prefix("client.type");

  public enum ClientType {
    TRANSPORT,
    REST
  }

  public static final String ELASTICSEARCH_WRITER_DEFAULT_HOST = "localhost";
  public static final int ELASTICSEARCH_WRITER_DEFAULT_PORT = 9300;
}
