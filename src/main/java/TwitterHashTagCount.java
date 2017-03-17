import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.redis.RedisSink;
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
            // SOURCE -> MAP (#hashtag, count)
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

            // (#hashtag, count) (hidden field: time)
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

            // ACC initialValue,
            // FoldFunctions <T, ACC>,
            // AllWindowFunction <ACC, R, W>,
            // TypeInformation<ACC>,
            // TypeInformation<R>
            // ACC = HashTagCount //accumulator
            // R = HashTagCount //ReturnType
            // W = TimeWindow //window
            // T = HashTagCount // initial Input
            //


//            window() |----------| fold() |-----|

            AllWindowedStream<HashTagCount, TimeWindow> windowedMap = stampedMap.keyBy("tag").sum("count").timeWindowAll(Time.seconds(300), Time.seconds(5));
            windowedMap.apply(new TopNTweetsAllWindowFunction()).print();
//            windowedMap.apply()
            /*windowedMap.apply(new AllWindowFunction<HashTagCount, HashTagCount, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<HashTagCount> values, Collector<HashTagCount> out) throws Exception {
                    PriorityQueue<HashTagCount> topN = new PriorityQueue<HashTagCount>(Comparator.comparingInt((hashTag) -> (hashTag.getCount())));
                    HashSet<String> encountered = new HashSet<>();
                    LOG.debug("in apply");
                    String previousKey = null;
                    int previousValue = 0;
                    for (HashTagCount tag : values) {

                        if (previousKey != null && !previousKey.equals(tag.getTag())) {
                            if (!encountered.add(previousKey)){
                                LOG.debug("Already encountered " + previousKey);
                            }
                            topN.add(new HashTagCount(previousKey, previousValue));
//                            LOG.debug("Added " + previousKey, previousValue);
                            if (topN.size() > 10){
                                topN.poll();
                            }
                            previousValue = 0;
                        }
                        previousKey = tag.getTag();
                        previousValue += tag.getCount();
                    }
                    LOG.debug("End of apply, size = " + topN.size());
                    for (HashTagCount hashTagCount : topN) {
                        out.collect(hashTagCount);
                    }
                }
            }).keyBy("count").print();

*/
                   /* .apply(
                    new AllWindowFunction<HashTagCount, HashTagCount, TimeWindow>() {


                        @Override
                        public void apply(TimeWindow window, Iterable<HashTagCount> values, Collector<HashTagCount> out) throws Exception {

                        }

//                        @Override
//                        public void apply(TimeWindow window, Iterable<HashTagCount> values, Collector<HashTagCount> out) throws Exception {
//                            PriorityQueue<HashTagCount> topN = new PriorityQueue<HashTagCount>(Comparator.comparingInt((hashTag) -> -(hashTag.getCount())));
//
//                            LOG.debug("in apply");
//                            String previousKey = null;
//                            int previousValue = 0;
//                            for (HashTagCount tag : values) {
//
//                                if (previousKey != null && !previousKey.equals(tag.getTag())) {
//                                    topN.add(new HashTagCount(previousKey, previousValue));
//                                    if (topN.size() > 10){
//                                        topN.poll();
//                                    }
//                                    previousValue = 0;
//                                }
//                                LOG.debug(previousKey);
//                                previousKey = tag.getTag();
//                                LOG.debug(previousKey);
//                                previousValue += tag.getCount();
//                            }
//                            LOG.debug("End of apply, size = " + topN.size());
//                            for (HashTagCount hashTagCount : topN) {
//                                out.collect(hashTagCount);
//                            }
//                        }

            }).print();*/
                    /*.addSink(new RedisMapper<HashTagCount>() {
                @Override
                public RedisCommandDescription getCommandDescription() {
                    return new RedisCommandDescription(RedisCommand.HSET, "topNHashTags");
                }

                @Override
                public String getKeyFromData(HashTagCount data) {
                    return data.getTag();
                }

                @Override
                public String getValueFromData(HashTagCount data) {
                    return Integer.toString(data.getCount());
                }
            }*/
//            windowedMap.fold(new HashTagCount(),
//
//                    new FoldFunction<HashTagCount, HashTagCount>() {
//                        @Override
//                        public HashTagCount fold(HashTagCount accumulator, HashTagCount value) throws Exception {
//
//                            if (accumulator.getTag() != null && !accumulator.getTag().equals(value.getTag())) {
//                                accumulator = new HashTagCount();
//                            }
//
//                            accumulator.setTag(value.getTag());
//                            accumulator.setCount(accumulator.getCount() + value.getCount());
//                            LOG.debug("Fold " + accumulator);
//
//                            return accumulator;
//                        }
//                    }
//
//            ).print();
                    //.keyBy("tag").sum("count").print();

//            windowedMap.fold(
//                        new HashTagCount(), //ACC initialValue
//                        (FoldFunction<HashTagCount, HashTagCount>) (accumulator, value) -> {
//                            if (accumulator.getTag() != null && !accumulator.getTag().equals(value.getTag())){
//                                accumulator = new HashTagCount();
//                            }
//                            accumulator.setTag(value.getTag());
//                            accumulator.setCount(accumulator.getCount() + value.getCount());
//                            LOG.debug("Fold " + accumulator);
//                            return accumulator;
//                        },
//                    (WindowFunction<HashTagCount, HashTagCount, Tuple, TimeWindow>) (tuple, window, input, out) -> {
//                        LOG.debug("Apply called");
//                        for (HashTagCount hashTag : input) {
//                            LOG.debug("Apply " + hashTag);
//                        }
//                    },
//                    stampedMap.getType(),
//                    stampedMap.getType()
//                );
//                    .print();
                        /*.fold(new HashTagCount(), (accumulator, value) -> {
                    accumulator.setCount(accumulator.getCount() + 1);
                    LOG.debug("Fold: " + value.toString());
                    return accumulator;
                }).print();*/

//                        .sum("count").print();
//            KeyedStream<HashTagCount, String> keyedMap = stampedMap.keyBy(new KeySelector<HashTagCount, String>() {
//                @Override
//                public String getKey(HashTagCount value) throws Exception {
//                    return value.getTag();
//                }
//            });
//
//            WindowedStream<HashTagCount, String, TimeWindow> windowedMap = keyedMap.timeWindow(Time.seconds(10), Time.seconds(5));
////            AllWindowedStream<HashTagCount, GlobalWindow> windowedMap = keyedMap.countWindowAll(100, 5);
//            windowedMap.sum("count").print();

////            WindowedStream<HashtagCount>, Tuple, TimeWindow> windowedMap =  keyedMap.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)));
//
//            // TODO Fix deze
///*            windowedMap.fold(
//                    new FoldApplyAllWindowFunction<GlobalWindow, HashtagCount, Integer>, HashtagCount, HashtagCount>(
//                            new HashtagCount(),
//                            new FoldFunction<HashtagCount, HashtagCount>() {
//                                @Override
//                                public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
//                                    return value;
//                                }
//                            },
//                            new AllWindowFunction<HashtagCount, HashtagCount, GlobalWindow>() {
//                                @Override
//                                public void apply(GlobalWindow window, Iterable<HashtagCount> values, Collector<HashtagCount> out) throws Exception {
//                                    for (HashtagCount keyedTuple : values) {
//                                        out.collect(keyedTuple);
//                                    }
//                                }
//                            }, windowedMap.getInputType()
//                    ));
//                   */
//
//
//
////            windowedMap.apply(new WindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, Tuple, GlobalWindow>() {
////                @Override
////                public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
////                    for (Tuple2<String, Integer> keyedTuple : input) {
////                        out.collect(keyedTuple);
////                    }
////                }
////            }).print();
//
//            windowedMap
//                    .apply(new AllWindowFunction<HashtagCount, HashtagCount, GlobalWindow>() {
//                        @Override
//                        public void apply(GlobalWindow window, Iterable<HashtagCount> values, Collector<HashtagCount> out) throws Exception {
//                            for (HashtagCount hashtag : values) {
//                                LOG.info("Apply: " + hashtag);
//                                out.collect(hashtag);
//                            }
//                        }
//                    }).print()
////                    .reduce(new PrintTweetsReduceFunction()).printToErr()
//
//
//                    ;



            env.execute("Twitter Streaming Test");
        } catch (Exception e) {
            e.printStackTrace();
        }









    }

    static class TopNTweetsAllWindowFunction implements AllWindowFunction<HashTagCount, HashTagCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<HashTagCount> values, Collector<HashTagCount> out) throws Exception {
            List<HashTagCount> entries = new LinkedList<>();


            for (HashTagCount tag : values) {
                LOG.debug(tag.toString());
                if (entries.size() == 0 ) {
                    entries.add(tag);
                }

                for (HashTagCount h : entries){
                    boolean found = false;
                    if (h.getTag().equals(tag.getTag())) {
                        if (h.getCount() > tag.getCount()) {
                            h.setCount(tag.getCount());
                            found = true;
                        }
                    }
                    if (!found) {
                        System.err.println("Found");

                        entries.add(tag);
                    } else {
                        System.err.println("Not found");
                    }
                }
            }


            entries.sort(Comparator.comparingInt(HashTagCount::getCount));
            int collectSize = entries.size() >= 10 ? 10 : entries.size();
            for (int i = 0; i < collectSize; i++) {
                out.collect(entries.get(i));
            }

            LOG.debug("End of apply, size = " + entries.size());
            entries.clear();
        }
    }

    static class PrintTweetsReduceFunction implements ReduceFunction<HashTagCount> {
        @Override
        public HashTagCount reduce (HashTagCount value1, HashTagCount value2) throws Exception {
            LOG.debug("Red: " + value1 + value2);
            return value2;
        }
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map ) {
        List<Map.Entry<K, V>> list =
                new LinkedList<Map.Entry<K, V>>( map.entrySet() );
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
            {
                return (o1.getValue()).compareTo( o2.getValue() );
            }
        } );

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put( entry.getKey(), entry.getValue() );
        }
        return result;
    }






}
