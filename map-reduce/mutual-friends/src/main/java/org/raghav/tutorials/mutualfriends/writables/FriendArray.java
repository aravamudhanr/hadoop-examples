package org.raghav.tutorials.mutualfriends.writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;

public class FriendArray extends ArrayWritable {

    /*
    Hadoop is using reflection and it can not guess any parameters to feed.
    We should provide an empty default constructor in your custom key writable class
    https://intellipaat.com/community/3216/no-such-method-exception-hadoop-init
     */
    public FriendArray() {
        super(Friend.class);
    }

    public FriendArray(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public FriendArray(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    @Override
    public boolean equals(Object object) {
        FriendArray friendArray = (FriendArray) object;
        Friend[] friend1 = Arrays.copyOf(this.get(), this.get().length, Friend[].class);
        Friend[] friend2 = Arrays.copyOf(friendArray.get(), friendArray.get().length, Friend[].class);
        boolean result = false;
        for (Friend outerFriend : friend1) {
            result = false;
            for (Friend innerFriend : friend2) {
                if (outerFriend.equals(innerFriend)) {
                    result = true;
                    break;
                }
            }
            if (!result) {
                return result;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        Friend[] friendArray = Arrays.copyOf(get(), get().length, Friend[].class);
        String print = "";
        for (Friend friend : friendArray) {
            print+=friend;
        }
        return print;
    }
}