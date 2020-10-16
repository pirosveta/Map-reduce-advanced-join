package ru.bmstu.hadoop;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair {
    private String firstKey, secondKey;

    public TextPair(String firstKey, String secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    public Object getFirst() {
        return firstKey;
    }

    public static class FirstPartitioner<K, V> extends Partitioner<K, V> {
        @Override
        public int getPartition(K key, V value, int numReduceTasks) {
            return key.hashCode() % numReduceTasks;
        }
    }

    public class FirstComparator implements RawComparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 0;
        }

        public int compare(WritableComparable o1, WritableComparable o2) {
            return 0;
        }
    }
}
