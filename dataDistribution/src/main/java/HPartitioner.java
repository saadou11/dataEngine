

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partitioner;

public class HPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected int numPartition;

	public HPartitioner(int numPartition) {
		this.numPartition = numPartition;
	}

	@Override
	public int getPartition(Object key) {
//		TryThis
		return Integer.parseInt(Bytes.toStringBinary((byte[]) key)) % numPartition;
		//return Bytes.toInt(Bytes.toBytes(key.toString())) % numPartition;
	}

	@Override
	public int numPartitions() {
		return numPartition;
	}

	protected final int hash(String key) {
		return (key == null) ? 0 : Math.abs(key.hashCode());
	}

}
