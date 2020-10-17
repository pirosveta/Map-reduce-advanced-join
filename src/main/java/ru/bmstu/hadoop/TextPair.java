package ru.bmstu.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair>{
    Text firstKey = new Text();
    Text secondKey = new Text();

    public TextPair(String firstKey, String secondKey) {
        this.firstKey = new Text(firstKey);
        this.secondKey = new Text(secondKey);
    }

    public Text getFirst() {
        return new Text(this.firstKey);
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

    @Override
    public int compareTo(TextPair other) {
        if (this.firstKey.equals(other.firstKey)) {
            return this.secondKey.compareTo(other.secondKey);
        }
        else return this.firstKey.compareTo(other.firstKey);
    }

    public static class FirstPartitioner extends Partitioner {
        @Override
        public int getPartition(Object o, Object o2, int numPartitions) {
            TextPair key = (TextPair) o;
            return (key.firstKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class FirstComparator extends WritableComparator {
        public FirstComparator() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TextPair firstObject = (TextPair) a;
            TextPair secondObject = (TextPair) b;
            return firstObject.firstKey.compareTo(secondObject.firstKey);
        }
    }
}
