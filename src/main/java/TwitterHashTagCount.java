import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

public class TwitterHashTagCount {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterHashTagCount.class);



    private static class FilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

        private final ArrayList<Long> followings;
        private final ArrayList<Location> locations;
        private ArrayList<String> languages;
        private ArrayList<String> trackTerms;



        FilterEndpoint(){

            this.languages = new ArrayList<>();
            this.trackTerms = new ArrayList<>();
            this.followings = new ArrayList<>();
            this.locations = new ArrayList<>();
        }

        public void addLanguage(String... language){
            Collections.addAll(this.languages, language);
        }

        public void addTrackTerm(String... trackTerm){
            Collections.addAll(this.trackTerms, trackTerm);
        }

        public void addLocation(Location... location){
            Collections.addAll(this.locations, location);
        }

        public void addFollowing(Long... id){
            Collections.addAll(this.followings, id);
        }



        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            if (languages.size() > 0) {
                endpoint.languages(languages);
            }
            if (followings.size() > 0) {
                endpoint.followings(followings);
            }
            if (locations.size() > 0) {
                endpoint.locations(locations);
            }
            if (trackTerms.size() > 0) {
                endpoint.trackTerms(trackTerms);
            }
            return endpoint;
        }
    }
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        try {

            FilterEndpoint filter = new FilterEndpoint();
            filter.addLanguage("nl", "en");
            filter.addTrackTerm("VVD", "PVV", "GroenLinks", "GL", "CDA", "PVDA", "SP",
                    "CU", "D66", "SGP", "PvdD", "50plus", "stemmen", "verkiezingen", "trump");


            Properties props = new Properties();

            props.setProperty(TwitterSource.CONSUMER_KEY, "Vhh9wtoeqzWf08BhwmeUXOTSB");
            props.setProperty(TwitterSource.CONSUMER_SECRET, "Ou1nHWLvIoxQE56mYDJl5JauwkY2N67NSfQYyO46MYjuYDnKIJ");
            props.setProperty(TwitterSource.TOKEN, "3221388387-VwffqrtFc3P0fHVDZPL8ZxpCuaSUguw2rbGoz23");
            props.setProperty(TwitterSource.TOKEN_SECRET, "7dhijHUDA4BBtnCxAdnQqlLiNPWVF2P3jRMmWTrKW2oJ4");

            TwitterSource twitterSource = new TwitterSource(props);
            twitterSource.setCustomEndpointInitializer(filter);


            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
            SingleOutputStreamOperator<HashTagCount> flatMapped = env.addSource(twitterSource, "Twitter")
                    .flatMap(new FlatMapFunction<String, HashTagCount>() {
                        @Override
                        public void flatMap(String value, Collector<HashTagCount> out) throws Exception {
                            try {
                                JSONArray hashTags = new JSONObject(value).getJSONObject("entities").getJSONArray("hashtags");
                                for (int i = 0; i < hashTags.length(); i++) {
                                    HashTagCount hashTag = new HashTagCount(hashTags.getJSONObject(i).getString("text").toLowerCase(),
                                            hashTags.getJSONObject(i).getJSONArray("indices").length() / 2);
                                    out.collect(hashTag);
                                }
                            } catch (JSONException | ArrayIndexOutOfBoundsException e) {/* Skip */}
                        }
                    });

            SingleOutputStreamOperator<HashTagCount> stampedMap = flatMapped.assignTimestampsAndWatermarks(
                    new AssignerWithPeriodicWatermarks<HashTagCount>() {
                        @Nullable
                        @Override
                        public Watermark getCurrentWatermark() {
                            return new Watermark(System.currentTimeMillis() - 1000L);
                        }

                        @Override
                        public long extractTimestamp(HashTagCount element, long previousElementTimestamp) {
                            return System.currentTimeMillis();
                        }
                    });


            AllWindowedStream<HashTagCount, TimeWindow> windowedMap = stampedMap.keyBy("tag").timeWindowAll(Time.seconds(300), Time.seconds(5));
            windowedMap
                    .apply(new TopNTweetsAllWindowFunction())
                    .addSink(
                            new RedisSink<>(new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build(),
                                    new RedisMapper<Tuple3<Integer, String, Integer>>() {
                                        @Override
                                        public RedisCommandDescription getCommandDescription() {
                                            return new RedisCommandDescription(RedisCommand.SET);
                                        }

                                        @Override
                                        public String getKeyFromData(Tuple3<Integer, String, Integer> data) {
                                            return "Top10-" + data.f0;
                                        }

                                        @Override
                                        public String getValueFromData(Tuple3<Integer, String, Integer> data) {
                                            return data.f1 + ", " + Integer.toString(data.f2);
                                        }
                                    }
                            ));
            env.execute("Twitter Streaming Test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class TopNTweetsAllWindowFunction implements AllWindowFunction<HashTagCount, Tuple3<Integer, String, Integer>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<HashTagCount> values, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
            List<HashTagCount> entries = new LinkedList<>();


            LOG.debug("Apply, new window");
            for (HashTagCount tag : values) {
                if (entries.size() == 0 ) {
                    entries.add(tag);
                }

                ListIterator<HashTagCount> iter = entries.listIterator();

                boolean modified = false;
                while (iter.hasNext()) {
                    HashTagCount h = iter.next();

                    if (h.getTag().equals(tag.getTag())) {
                        iter.set(new HashTagCount(tag.getTag(), h.getCount() + tag.getCount()));
                        modified = true;
                        break;
                    }
                }
                if (!modified) {
                    iter.add(tag);
                }
            }


            entries.sort((o1, o2) -> o2.getCount() - o1.getCount());

            int collectSize = entries.size() >= 10 ? 10 : entries.size();
            for (int i = 0; i < collectSize; i++) {
                LOG.info((i + 1) + ") " + entries.get(i));
                out.collect(new Tuple3<>((i+1), entries.get(i).getTag(), entries.get(i).getCount()));
            }

            entries.clear();
        }
    }


}
