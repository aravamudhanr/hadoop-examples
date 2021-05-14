package org.raghav.tutorials.mutualfriends.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendPair implements WritableComparable {

    private Friend first;
    private Friend second;

    /*
    Hadoop is using reflection and it can not guess any parameters to feed.
    We should provide an empty default constructor in your custom key writable class
    https://intellipaat.com/community/3216/no-such-method-exception-hadoop-init
     */
    public FriendPair() {
        this.first = new Friend();
        this.second = new Friend();
    }

    public FriendPair(Friend first, Friend second) {
        this.first = first;
        this.second = second;
    }

    public Friend getFirst() {
        return first;
    }

    public void setFirst(Friend first) {
        this.first = first;
    }

    public Friend getSecond() {
        return second;
    }

    public void setSecond(Friend second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int compareTo(Object object) {
        FriendPair friendPair = (FriendPair) object;
        int cmp = -1;
        if (getFirst().compareTo(friendPair.getFirst()) == 0 || getFirst().compareTo(friendPair.getSecond()) == 0) {
            cmp = 0;
        }
        if (cmp != 0) {
            return cmp;
        }
        cmp = -1;
        if (getSecond().compareTo(friendPair.getFirst()) == 0 || getSecond().compareTo(friendPair.getSecond()) == 0) {
            cmp = 0;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object object) {
        FriendPair friendPair = (FriendPair) object;
        boolean eq = getFirst().equals(friendPair.getFirst()) || getFirst().equals(friendPair.getSecond());
        if (!eq) {
            return eq;
        }
        return getSecond().equals(friendPair.getFirst()) || getSecond().equals(friendPair.getSecond());
    }

    @Override
    public String toString() {
        return "[ " + first + ";" + second + "]";
    }

    @Override
    public int hashCode() {
        return first.getId().hashCode() + second.getId().hashCode();
    }
}