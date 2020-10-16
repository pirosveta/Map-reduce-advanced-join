package ru.bmstu.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text nameOfAirport = new Text(iter.next());
        double minDelay = Integer.MAX_VALUE, maxDelay = 0, avgDelay = 0, numberOfDelay = 0;
        while (iter.hasNext()) {
            double delay = Double.parseDouble(iter.next().toString());
            if (delay < minDelay) minDelay = delay;
            if (delay > maxDelay) maxDelay = delay;
            avgDelay += delay;
            numberOfDelay++;
            System.out.println("DA");
        }
        avgDelay /= numberOfDelay;
        Text outValue = new Text(nameOfAirport + "\t" + minDelay + "\t" + maxDelay + "\t" + avgDelay);
        context.write(key.getFirst(), outValue);
    }
}