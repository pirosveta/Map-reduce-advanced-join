package ru.bmstu.hadoop;


import org.apache.hadoop.mapreduce.Partitioner;

public class HashPartitioner extends Partitioner {
    @Override
    public int getPartition(Object o, Object o2, int numPartitions) {
        TextPair key = (TextPair) o;
        return (key.firstKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
