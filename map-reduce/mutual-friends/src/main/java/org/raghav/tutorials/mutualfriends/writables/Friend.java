package org.raghav.tutorials.mutualfriends.writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Friend implements WritableComparable {

    private IntWritable id;
    private Text name;
    private Text homeTown;

    /*
    Hadoop is using reflection and it can not guess any parameters to feed.
    We should provide an empty default constructor in your custom key writable class
    https://intellipaat.com/community/3216/no-such-method-exception-hadoop-init
     */
    public Friend() {
        this.id = new IntWritable();
        this.name = new Text();
        this.homeTown = new Text();
    }

    public Friend(IntWritable id, Text name, Text homeTown) {
        this.id = id;
        this.name = name;
        this.homeTown = homeTown;
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public Text getHomeTown() {
        return homeTown;
    }

    public void setHomeTown(Text homeTown) {
        this.homeTown = homeTown;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        name.write(dataOutput);
        homeTown.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        name.readFields(dataInput);
        homeTown.readFields(dataInput);
    }

    @Override
    public int compareTo(Object object) {
        Friend friend = (Friend) object;
        return getId().compareTo(friend.getId());
    }

    @Override
    public boolean equals(Object object) {
        Friend friend = (Friend) object;
        return getId().equals(friend.getId());
    }

    @Override
    public String toString() {
        return id + ":" + name + " ";
    }
}