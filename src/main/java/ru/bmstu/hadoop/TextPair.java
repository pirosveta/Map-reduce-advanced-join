package ru.bmstu.hadoop;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair {
    public static class FirstPartitioner<K, V> extends Partitioner<K, V> {
        @Override
        public int getPartition(K key, V value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class FirstComparator extends ByteWritable.Comparator {

    }
}
