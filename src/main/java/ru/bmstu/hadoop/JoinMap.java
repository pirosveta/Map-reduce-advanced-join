package ru.bmstu.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class MapJoinMapper extends MapReduceBase implements
        Mapper<Text, TupleWritable, Text, Text> {
    @Override
    public void map(Text key, TupleWritable value,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
        Text call = (Text) value.get(0);
        Text system = (Text) value.get(1);
        output.collect(call, system);
    }
}