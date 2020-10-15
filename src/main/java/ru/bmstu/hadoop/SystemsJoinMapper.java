package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;

public class SystemsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, WrappedReducer.Context context) throws IOException, InterruptedException {
        if ();
        String[] columns = value.toString().split(",");
        String destAirportId = columns[14], delay = columns[17];
        context.write(new TextPair(destAirportId, "1"), new Text(delay));
    }
}