package ru.bmstu.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements Writable, WritableComparable{
    Text firstKey = new Text();
    Text secondKey = new Text();


    public TextPair(Text firstKey, Text secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    public Object getFirst() {
        return firstKey;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

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

        }
    }
}
