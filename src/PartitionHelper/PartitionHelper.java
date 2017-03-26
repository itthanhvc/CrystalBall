package PartitionHelper;

import org.apache.hadoop.mapreduce.Partitioner;

import Hybrid.Neighbor;
import Pair.MultipleKey;

public class PartitionHelper<K,V> extends Partitioner<K, V>{

	@Override
	public int getPartition(K key, V arg1, int numReduceTasks) {
		if(key.getClass() == Neighbor.class)
			return Integer.parseInt(((Neighbor)key).getPrimary().toString())% numReduceTasks;
		if(key.getClass() == MultipleKey.class)
			return Integer.parseInt(((MultipleKey)key).getKey().toString())% numReduceTasks;
		return Integer.parseInt(key.toString())% numReduceTasks;
	}
	
	public static int getReducerNumber(){
		return 3;
	}
}