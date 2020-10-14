package ru.bmstu.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class TextPair{
    public static class FirstPartitioner extends Partitioner {

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
