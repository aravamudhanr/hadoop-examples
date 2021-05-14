package org.raghav.tutorials.mutualfriends;

import org.apache.hadoop.mapreduce.Reducer;
import org.raghav.tutorials.mutualfriends.writables.Friend;
import org.raghav.tutorials.mutualfriends.writables.FriendArray;
import org.raghav.tutorials.mutualfriends.writables.FriendPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MutualFriendsReducer extends Reducer<FriendPair, FriendArray, FriendPair, FriendArray> {

    @Override
    public void reduce(FriendPair key, Iterable<FriendArray> values, Context context) throws IOException, InterruptedException {

        List<Friend[]> friendArrayList = new ArrayList<>();
        for(FriendArray friendArray : values) {
            Friend[] friends = Arrays.copyOf(friendArray.get(), friendArray.get().length, Friend[].class);
            friendArrayList.add(friends);
        }

        if (friendArrayList.size() != 2) {
            return;
        }

        List<Friend> commonFriendList = new ArrayList<>();
        for(Friend outerFriend : friendArrayList.get(0)) {
            for (Friend innerFriend : friendArrayList.get(1)) {
                if (outerFriend.equals(innerFriend)) {
                    commonFriendList.add(innerFriend);
                }
            }
        }

        Friend[] commonFriends = Arrays.copyOf(commonFriendList.toArray(), commonFriendList.toArray().length, Friend[].class);
        FriendArray commonFriendArray = new FriendArray(Friend.class, commonFriends);
        context.write(key, commonFriendArray);
    }
}