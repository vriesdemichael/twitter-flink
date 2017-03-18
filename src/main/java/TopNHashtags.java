import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

public class TopNHashtags {

    private static final Logger LOG = LoggerFactory.getLogger(TopNHashtags.class);
    private static int n;

    public static void main(String[] args) throws Exception {

        //Get parameters from properties file or args.
        ParameterTool parameter;

        String propertiesPath = "TopNHashtags.properties";
        File f = new File(propertiesPath);
        if(f.exists() && !f.isDirectory()) {
            LOG.debug("Found parameters in " + propertiesPath);
            parameter = ParameterTool.fromPropertiesFile(propertiesPath);
        } else {
            LOG.debug("Using args[] as parameters");
            parameter = ParameterTool.fromArgs(args);
        }

        final String redisHost = parameter.get("redisHost", "localhost");
        final int redisPort = parameter.getInt("redisPort", 6379);
        final int windowSize = parameter.getInt("windowSize", 300);
        final int windowSlide = parameter.getInt("windowSlide", 60);
        n = parameter.getInt("N", 10);


        // Initialize Twitter
        TwitterFilterEndpoint filter = new TwitterFilterEndpoint();

        if (parameter.has("language")) {
            filter.addLanguage(parameter.get("language").replace(" ","").split(","));
        } else {
            filter.addLanguage("nl", "en");
        }


        if (parameter.has("track")) {
            filter.addTrackTerm(parameter.get("track").replace(" ", "").split(","));
        } else {
            filter.addTrackTerm("VVD", "PVV", "GroenLinks", "GL", "CDA", "PVDA", "SP",
                    "CU", "D66", "SGP", "PvdD", "50plus", "stemmen", "verkiezingen", "trump");
        }

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, parameter.getRequired("twitterConsumerKey"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, parameter.getRequired("twitterConsumerSecret"));
        props.setProperty(TwitterSource.TOKEN, parameter.getRequired("twitterToken"));
        props.setProperty(TwitterSource.TOKEN_SECRET, parameter.getRequired("twitterSecret"));

        TwitterSource twitter = new TwitterSource(props);
        twitter.setCustomEndpointInitializer(filter);


        /*
         * Flink streaming Overview:
         *
         * env
         *      .addSource(...)
         *      .flatmap(...)
         *      .returns(...)
         *      .assignTimestampsAndWatermarks(...)
         *      .timeWindowAll(...)
         *      // Stream splits up into time windows here
         *      .apply(...)
         *      .addSink(...)
         *
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setMaxParallelism(5);
        env.setParallelism(5);
        env
            /*
             * Get Data from the TwitterSource
             * Returns a JSON String representation of a status update
             */
            .addSource(twitter, "Twitter")

            /*
             * JSON String -> multiple (<hashtag>, <count>)
             * Maps the hashtags listed in the JSON String.
             */
            .flatMap((FlatMapFunction<String, HashTagCount>) (value, out) -> {
                try {
                    JSONArray hashTags = new JSONObject(value).getJSONObject("entities").getJSONArray("hashtags");
                    for (int i = 0; i < hashTags.length(); i++) {
                        HashTagCount hashTag = new HashTagCount(hashTags.getJSONObject(i).getString("text").toLowerCase(),
                                hashTags.getJSONObject(i).getJSONArray("indices").length() / 2);
                        out.collect(hashTag);
                    }
                } catch (JSONException | ArrayIndexOutOfBoundsException e) {/* Skip */}
            })

            /* A helper function for previous flatmap lambda */
            .returns(HashTagCount.class)

            /*
             * Assign timestamps and Watermarks, these are needed to make use of a TimeWindow
             * A better way is to do this is the Source, but TwitterSource does not do this.
             */
            .assignTimestampsAndWatermarks(
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
                })

            /*
             *
             * Create a sliding time window, the first value is the length of the time window, the second
             * is the interval at which a new time window is started.
             *
             *       0        60      120     180     240     300     360     420
             *       |---------------Window 1----------------|
             *               |---------------Window 2----------------|
             *                       |---------------Window 3----------------|
             */
            .timeWindowAll(Time.seconds(windowSize), Time.seconds(windowSlide))

            /*
             * The apply function specified in TopNTweetsAllWindowFunction is called. It implements
             * "AllWindowFunction" which means that at the end of every window this function is called,
             * creating a new stream.
             * With a window size of 300 seconds and a window interval of 60 seconds there will be 5 active
             * windows.
             */
            .apply(new TopNTweetsAllWindowFunction())

            /*
             * The datastream is saved to a Sink, in this case Redis.
             * For quick tests in your IDE you can replace .addSink(...) with .print()
             */
            .addSink(
                new RedisSink<>(
                    new FlinkJedisPoolConfig.Builder().setHost(redisHost).setPort(redisPort).build(),
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

        /*
         *  Actually execute the streaming plan defined above.
         *  Everything up till this point was lazy execution.
         */
        env.execute("Twitter Top 10 hashtags");

    }

    /**
     * Apply function for aggregating Tweet counts within a window.
     * Listed seperately because of its size.
     */
    static class TopNTweetsAllWindowFunction implements AllWindowFunction<HashTagCount, Tuple3<Integer, String, Integer>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<HashTagCount> values, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
            LOG.debug("Window finished");

            // List of all entries in this window.
            List<HashTagCount> entries = new LinkedList<>();


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

            int collectSize = entries.size() >= n ? n : entries.size();
            for (int i = 0; i < collectSize; i++) {
                LOG.info((i + 1) + ") " + entries.get(i));
                out.collect(new Tuple3<>((i+1), entries.get(i).getTag(), entries.get(i).getCount()));
            }

            entries.clear();
        }
    }


}
