package ru.bmstu.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    protected void reduce(TextPair key, Iterable<Text> values, WrappedReducer.Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text nameOfAirport = new Text(iter.next());
        int minDelay = Integer.MAX_VALUE, maxDelay = 0, numberOfDelay = 0;
        double avgDelay = 0;
        while (iter.hasNext()) {
            int delay = Integer.parseInt(iter.next().toString());
            if (delay < minDelay) minDelay = delay;
            if (delay > maxDelay) maxDelay = delay;
            avgDelay += delay;
            numberOfDelay++;
        }
        avgDelay /= numberOfDelay;
        Text outValue = new Text(nameOfAirport + "\t" + minDelay + "\t" + maxDelay + "\t" + avgDelay);
        context.write(key.getFirst(), outValue);
    }
}