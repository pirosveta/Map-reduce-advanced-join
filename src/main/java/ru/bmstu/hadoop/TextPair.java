package ru.bmstu.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements Writable, WritableComparable<TextPair>{
    Text firstKey = new Text();
    Text secondKey = new Text();

    public TextPair(String firstKey, String secondKey) {
        this.firstKey = new Text(firstKey);
        this.secondKey = new Text(secondKey);
    }

    public Object getFirst() {
        return this.firstKey;
    }

    @Override
    public int compareTo(TextPair other) {
        if (this.firstKey.equals(other.firstKey)) {
            return this.secondKey.compareTo(other.secondKey);
        }
        else return this.firstKey.compareTo(other.firstKey);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.firstKey.write(out);
        this.secondKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.firstKey.readFields(in);
        this.secondKey.readFields(in);
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
