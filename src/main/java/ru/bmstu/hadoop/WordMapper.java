package ru.bmstu.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final String REPLACED_STRING = "[^a-zA-Zа-яА-ЯёЁ0-9\\s]", REPLACEMENT_STRING = "";

    public static class StringTools {
        public static String removeAllNonSymbols(String line) {
            return line.replaceAll(REPLACED_STRING, REPLACEMENT_STRING);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(StringTools.removeAllNonSymbols(line).toLowerCase());
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
