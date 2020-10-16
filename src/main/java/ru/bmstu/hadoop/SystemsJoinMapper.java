package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;

public class SystemsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    protected void map(LongWritable key, Text value, WrappedReducer.Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");
            String destAirportId = columns[0], nameAirport = columns[1];
            context.write(new TextPair(destAirportId,"0"), new Text(nameAirport));
        }
    }
}