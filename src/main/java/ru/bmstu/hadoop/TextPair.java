package ru.bmstu.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class TextPair extends org.apache.hadoop.mapreduce.Partitioner {
    @Override
    public int getPartition(Object o, Object o2, int numPartitions) {
        return 0;
    }

    public static class FirstPartitioner implements Partitioner {

        @Override
        public int getPartition(Object key, Object value, int numPartitions) {
            return 0;
        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static class FirstComparator {

    }
}
