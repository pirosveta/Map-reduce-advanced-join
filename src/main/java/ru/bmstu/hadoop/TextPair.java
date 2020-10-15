package ru.bmstu.hadoop;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair {
    private String firstKey, secondKey;
    
    public TextPair(String firstKey, String secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    public static class FirstPartitioner<K, V> extends Partitioner<K, V> {
        @Override
        public int getPartition(K key, V value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class FirstComparator extends ByteWritable.Comparator {

    }
}
