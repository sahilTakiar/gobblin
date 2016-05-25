package gobblin.writer.partitioner;

import com.google.common.base.Optional;
import com.google.gson.JsonElement;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;

import java.util.List;


public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<JsonElement> {

  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";

  private final Optional<List<String>> partitionColumns;

  public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);
  }

  private static Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
    return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
  }

  @Override
  public long getRecordTimestamp(JsonElement record) {
    return 0;
  }
}
