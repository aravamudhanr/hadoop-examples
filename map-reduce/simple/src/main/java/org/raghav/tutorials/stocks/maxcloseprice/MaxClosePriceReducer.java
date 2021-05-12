package org.raghav.tutorials.stocks.maxcloseprice;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxClosePriceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("key="+key);
        float maxClosePrice = Float.MIN_VALUE;
        for (FloatWritable value : values) {
            System.out.println("value = " + value);
            maxClosePrice = Math.max(maxClosePrice, value.get());
        }
        context.write(key, new FloatWritable(maxClosePrice));
    }
}