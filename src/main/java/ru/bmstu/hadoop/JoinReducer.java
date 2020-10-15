package ru.bmstu.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;

import java.io.IOException;
import java.util.Iterator;

public abstract class JoinReducer implements Reducer<TextPair, Text, Text, Text> {
    protected void reduce(TextPair key, Iterable<Text> values, WrappedReducer.Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text systemInfo = new Text(iter.next());
        while (iter.hasNext()) {
            Text call = iter.next();
            Text outValue = new Text(call.toString() + "\t" + systemInfo.toString());
            context.write(key.getFirst(), outValue);
        }
    }
}