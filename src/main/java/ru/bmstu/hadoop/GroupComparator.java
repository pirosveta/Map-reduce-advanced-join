package ru.bmstu.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    public GroupComparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextPair firstObject = (TextPair) a;
        TextPair secondObject = (TextPair) b;
        return firstObject.firstKey.compareTo(secondObject.firstKey);
    }
}
