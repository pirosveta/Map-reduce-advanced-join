package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final String REGEX = "\"", REPLACE = "", DELIMITER = ",", SECOND_KEY = "0";
    private static final int COLUMN_ID = 0, COLUMN_NAME = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(DELIMITER, 2);
            String destAirportId = columns[COLUMN_ID].replaceAll(REGEX, REPLACE), nameAirport = columns[COLUMN_NAME];
            context.write(new TextPair(destAirportId, SECOND_KEY), new Text(nameAirport));
        }
    }
}