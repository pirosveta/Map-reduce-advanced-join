package ru.bmstu.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CallsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final int COLUMN_ID = 14, COLUMN_DELAY = 17;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");
            try {
                if (Double.parseDouble(columns[COLUMN_DELAY]) > 0) {
                    String destAirportId = columns[COLUMN_ID], delay = columns[COLUMN_DELAY];
                    context.write(new TextPair(destAirportId, "1"), new Text(delay));
                }
            }
            catch (NumberFormatException e) {
            }
        }
    }
}