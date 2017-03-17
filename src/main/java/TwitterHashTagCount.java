import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

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


            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapped = env.addSource(twitterSource, "Twitter")
                    .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        try {
                            JSONArray hashtags =  new JSONObject(value).getJSONObject("entities").getJSONArray("hashtags");
                            for (int i = 0; i < hashtags.length(); i++) {
                                out.collect(new Tuple2<>(
                                        hashtags.getJSONObject(i).getString("text").toLowerCase(),
                                        hashtags.getJSONObject(i).getJSONArray("indices").length()/2
                                ));
                            }
                        } catch(JSONException | ArrayIndexOutOfBoundsException e){/* Skip */}
                    }
                });

                SingleOutputStreamOperator<Tuple2<String, Integer>> stampedMap = flatMapped.assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>() {

                            @Nullable
                            @Override
                            public Watermark getCurrentWatermark() {
                                return null;
                            }

                            @Override
                            public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
                                return System.currentTimeMillis();
                            }
                        });

//            KeyedStream<Tuple2<String, Integer>, Tuple> keyedMap = stampedMap.keyBy(0).sum(1).keyBy(0);

//            keyedMap.print();

//            SingleOutputStreamOperator<Tuple2<String, Integer>> summedMap = keyedMap
//                    .sum(1);
//            summedMap.print();

            AllWindowedStream<Tuple2<String, Integer>, GlobalWindow> windowedMap = stampedMap.countWindowAll(100, 5);
//            WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedMap =  keyedMap.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)));

            // TODO Fix deze
/*            windowedMap.fold(
                    new FoldApplyAllWindowFunction<GlobalWindow, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>(
                            new Tuple2<String, Integer>(),
                            new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                                @Override
                                public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                                    return value;
                                }
                            },
                            new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow>() {
                                @Override
                                public void apply(GlobalWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                                    for (Tuple2<String, Integer> keyedTuple : values) {
                                        out.collect(keyedTuple);
                                    }
                                }
                            }, windowedMap.getInputType()
                    ));
                   */



//            windowedMap.apply(new WindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, Tuple, GlobalWindow>() {
//                @Override
//                public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                    for (Tuple2<String, Integer> keyedTuple : input) {
//                        out.collect(keyedTuple);
//                    }
//                }
//            }).print();

//            windowedMap
//                    .reduce(new PrintTweetsReduceFunction()).printToErr()
//
//
//                    ;



            env.execute("Twitter Streaming Test");
        } catch (Exception e) {
            e.printStackTrace();
        }









    }

    static class PrintTweetsReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce (Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            LOG.debug("Red: " + value1 + value2);
            System.out.println("red" + value1 + value2);
            return value2;
        }
    }






}
