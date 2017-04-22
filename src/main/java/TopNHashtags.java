import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
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

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import static java.lang.Math.toIntExact;

public class TopNHashtags {

    private static final Logger LOG = LoggerFactory.getLogger(TopNHashtags.class);
    private static int n;

    public static void main(String[] args) throws Exception {

        //Get parameters from properties file or args.
        ParameterTool parameter;

        String propertiesPath = "TopNHashtags.properties";
        File f = new File(TopNHashtags.class.getResource(propertiesPath).getFile());
        if(f.exists() && !f.isDirectory()) {
            LOG.debug("Found parameters in " + propertiesPath);
            parameter = ParameterTool.fromPropertiesFile(TopNHashtags.class.getResource(propertiesPath).getFile());
            parameter = parameter.mergeWith(ParameterTool.fromArgs(args));
        } else {
            LOG.debug("Using args[] as parameters");
            parameter = ParameterTool.fromArgs(args);
        }

        LOG.debug("Parameters read: " + parameter.toMap().toString());

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

        if (parameter.has("maxParallelism")) {
            env.setMaxParallelism(parameter.getInt("maxParallelism"));
            LOG.debug("max parallelism set to " + env.getMaxParallelism());
        }
        if (parameter.has("setParallelism")) {
            env.setParallelism(parameter.getInt("setParallelism"));
            LOG.debug("parallelism set to " + env.getParallelism());
        }


        LOG.debug(String.format("Starting TopNHashtags with: N=%d, window size=%d, window slide=%d, redis=%s%d", n, windowSize, windowSlide, redisHost, redisPort));


        SingleOutputStreamOperator<Tuple3<Long, String, String>> mappedStatuses = env
            /*
             * Get Data from the TwitterSource
             * Returns a JSON String representation of a status update
             */
            .addSource(twitter, "Twitter")
            /*
             * JSON String -> multiple (<hashtag>, <count>)
             * Maps the hashtags listed in the JSON String.
             */
            .flatMap(new FlatMapFunction<String, Tuple3<Long, String, String>>() {
                @Override
                public void flatMap(String value, Collector<Tuple3<Long, String, String>> collector) throws Exception {
                    try {
                        JSONObject status = new JSONObject(value);
                        String statusText = status.getString("text");
                        Long statusID = status.getLong("id");

                        JSONArray hashTags = status.getJSONObject("entities").getJSONArray("hashtags");
                        StringBuilder hashTagString = new StringBuilder();
                        for (int i = 0; i < hashTags.length(); i++) {
                            if (hashTagString.length() > 0) {
                                hashTagString.append(" ");
                            }
                            hashTagString.append(hashTags.getJSONObject(i).getString("text").toLowerCase());
                        }

                        collector.collect(new Tuple3<Long, String, String>(statusID, statusText, hashTagString.toString()));

                    } catch (JSONException | ArrayIndexOutOfBoundsException e) {/* Skip */}
                }
            })
            /*
             * Assign timestamps and Watermarks, these are needed to make use of a TimeWindow
             * A better way is to do this is the Source, but TwitterSource does not do this.
             */
            .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>>() {
                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(System.currentTimeMillis() - 1000L);
                }

                @Override
                public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
                    return System.currentTimeMillis();
                }
            });


        SingleOutputStreamOperator<Tuple1<String>> tagsOnly = mappedStatuses.project(2);

        SingleOutputStreamOperator<Tuple3<Integer, String, Long>> topNCalc = tagsOnly.flatMap(new FlatMapFunction<Tuple1<String>, Tuple2<String, Long>>() {
            @Override
            public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String tag : value.f0.trim().split(" ")) {
                    if (tag.equals(" ") || tag.equals("")) {
                        continue;
                    }
                    out.collect(new Tuple2<>(tag, 1L));
                }
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
                .apply(new TopNTweetsAllWindowFunction());


        /*
         * The datastream is saved to a Sink, in this case Redis.
         * For quick tests in your IDE you can replace .addSink(...) with .print()
         */
        topNCalc.addSink(
            new RedisSink<>(
                new FlinkJedisPoolConfig.Builder().setHost(redisHost).setPort(redisPort).build(),
                new RedisMapper<Tuple3<Integer, String, Long>>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.SET);
                    }

                    @Override
                    public String getKeyFromData(Tuple3<Integer, String, Long> data) {
                        return "Top10-" + data.f0;
                    }

                    @Override
                    public String getValueFromData(Tuple3<Integer, String, Long> data) {
                        return data.f1 + ", " + Long.toString(data.f2);
                    }
                }
            )
        ).name("Redis_" + redisHost + ":" + redisPort);

        SingleOutputStreamOperator<Tuple2<Long, String>> topNTexts = mappedStatuses.connect(topNCalc).flatMap(
            new CoFlatMapFunction<Tuple3<Long, String, String>, Tuple3<Integer, String, Long>, Tuple2<Long, String>>() {
                String[] topN;

                @Override
                public void flatMap1(Tuple3<Long, String, String> value, Collector<Tuple2<Long, String>> out) throws Exception {
                    for (String tag : value.f2.split(" ")) {
                        if (ArrayUtils.contains(topN, tag)) {
                            out.collect(new Tuple2<>(value.f0, value.f1));
                            break;
                        }
                    }
                }

                @Override
                public void flatMap2(Tuple3<Integer, String, Long> value, Collector<Tuple2<Long, String>> out) throws Exception {
                    if (topN == null) {
                        topN = new String[n];
                    }
                    topN[value.f0 - 1] = value.f1;
                }
            }
        );



        topNTexts.map(value -> "Status: \t" + value.f1).returns(String.class).print();

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
    static class TopNTweetsAllWindowFunction implements AllWindowFunction<Tuple2<String, Long>, Tuple3<Integer, String, Long>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<String, Long>> values, Collector<Tuple3<Integer, String, Long>> out) throws Exception {
            LOG.debug("Window finished");

            // List of all entries in this window.
            List<Tuple2<String, Long>> entries = new LinkedList<>();


            for (Tuple2<String, Long> tag : values) {
                if (entries.size() == 0 ) {
                    entries.add(tag);
                }

                ListIterator<Tuple2<String, Long>> iter = entries.listIterator();

                boolean modified = false;
                while (iter.hasNext()) {
                    Tuple2<String, Long> h = iter.next();

                    if (h.f0.equals(tag.f0)) {
                        iter.set(new Tuple2<>(tag.f0, h.f1 + tag.f1));
                        modified = true;
                        break;
                    }
                }
                if (!modified) {
                    iter.add(tag);
                }
            }

            entries.sort((o1, o2) -> toIntExact(o2.f1 - o1.f1));

            int collectSize = entries.size() >= n ? n : entries.size();
            for (int i = 0; i < collectSize; i++) {
                LOG.info((i + 1) + ") " + entries.get(i));
                out.collect(new Tuple3<>((i+1), entries.get(i).f0, entries.get(i).f1));
            }

            entries.clear();
        }
    }


}
