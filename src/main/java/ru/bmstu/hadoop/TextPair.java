package ru.bmstu.hadoop;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair {
    public static class FirstPartitioner extends Partitioner {
        @Override
        public int getPartition(Object o, Object o2, int numPartitions) {
            return 0;
        }
    }

    public static class FirstComparator extends ByteWritable.Comparator {

    }
}
