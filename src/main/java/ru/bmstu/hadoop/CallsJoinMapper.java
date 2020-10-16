package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;

public class CallsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");
            if (Integer.parseInt(columns[17]) > 0) {
                String destAirportId = columns[14], delay = columns[17];
                context.write(new TextPair(destAirportId, "1"), new Text(delay));
            }
        }
    }
}