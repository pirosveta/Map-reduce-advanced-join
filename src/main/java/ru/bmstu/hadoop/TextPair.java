package ru.bmstu.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair>{
    Text firstKey = new Text();
    Text secondKey = new Text();

    public TextPair() {
        this.firstKey = new Text("");
        this.secondKey = new Text("");
    }

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
}
