package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;

public class SystemsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, WrappedReducer.Context context) throws IOException, InterruptedException {
        
    }
}