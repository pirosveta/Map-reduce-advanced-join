package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SystemsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final REGEX = "\"", REPLACE = "";
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",", 2);
            String destAirportId = columns[0].replaceAll(REGEX, REPLACE), nameAirport = columns[1];
            context.write(new TextPair(destAirportId, "0"), new Text(nameAirport));
        }
    }
}