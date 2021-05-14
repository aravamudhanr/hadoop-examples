package org.raghav.tutorials.mutualfriends;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.raghav.tutorials.mutualfriends.writables.Friend;
import org.raghav.tutorials.mutualfriends.writables.FriendArray;
import org.raghav.tutorials.mutualfriends.writables.FriendPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class MutualFriendsMapper extends Mapper<LongWritable, Text, FriendPair, FriendArray> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(),"|");
        String personJsonString = stringTokenizer.nextToken();
        String friendsJsonString = stringTokenizer.nextToken();

        Friend friend1 = populateFriend(personJsonString);
        List<Friend> friendList = populateFriendList(friendsJsonString);
        Friend[] friends = Arrays.copyOf(friendList.toArray(), friendList.toArray().length, Friend[].class);
        FriendArray friendArray = new FriendArray(Friend.class, friends);

        for (Friend friend2 : friendList) {
            FriendPair friendPair = new FriendPair(friend1, friend2);
            context.write(friendPair, friendArray);
        }
    }

    public Friend populateFriend(String personJsonString) {
        Friend friend = null;
        JSONParser jsonParser = new JSONParser();
        try {
            Object object = jsonParser.parse(personJsonString);
            friend = getFriendObject(object);
        } catch (ParseException parseException) {
            parseException.printStackTrace();
        }
        return friend;
    }

    public Friend getFriendObject(Object object) {
        JSONObject jsonObject = (JSONObject) object;
        Long idLong = (Long) jsonObject.get("id");
        IntWritable id = new IntWritable(idLong.intValue());
        Text name = new Text((String) jsonObject.get("name"));
        Text homeTown = new Text((String) jsonObject.get("homeTown"));
        return new Friend(id, name, homeTown);
    }

    public List<Friend> populateFriendList(String friendsJsonString) {
        List<Friend> friendList = new ArrayList<>();
        JSONParser jsonParser = new JSONParser();
        try {
            Object object = jsonParser.parse(friendsJsonString);
            JSONArray jsonArray = (JSONArray) object;
            for (Object jsonArrayObject : jsonArray) {
                Friend friend = getFriendObject(jsonArrayObject);
                friendList.add(friend);
            }
        } catch (ParseException parseException) {
            parseException.printStackTrace();
        }
        return friendList;
    }
}