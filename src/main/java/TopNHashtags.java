import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
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

        /*
         * Configuration
         */

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

        /*
         * Start of streaming setup
         */
        LOG.debug(String.format("Starting TopNHashtags with: N=%d, window size=%d, window slide=%d, redis=%s%d",
                n, windowSize, windowSlide, redisHost, redisPort));

        // Map the statuses.
        DataStream<Tuple3<Long, String, String>> mappedStatuses = env
            .addSource(twitter, "Twitter")
            .flatMap(new MapStatuses())
            .assignTimestampsAndWatermarks(new AttachCurrentTime());

        // Calculate the top N tags for the given time window.
        DataStream<Tuple3<Integer, String, Long>> topNTags = mappedStatuses
                // ("tag1 tag2 tag3") -> [(tag1), (tag2), (tag3)]
                .flatMap(new FlatMapTags())
                // Sliding time window
                .timeWindowAll(Time.seconds(windowSize), Time.seconds(windowSlide))
                // Reduce and calculate the top N tags
                .apply(new CalcTopNTags())
                .forceNonParallel()
                // Ensure that all nodes receive the new Top N
                .broadcast();


        // Connect the output of topNCalc to the mapped statuses stream
        SingleOutputStreamOperator<Tuple2<Long, String>> topNTexts = mappedStatuses
                .connect(topNTags)
                .flatMap(new FilterTopNCoFlatMap());

        // Print the statuses which contain a top N tag.
        topNTexts
                // Take only the text value as String
                .map(value -> value.f1)
                // Helper function for lambda functions in java (not necessary when using Scala)
                .returns(String.class)
                .print();


        // Store the top N to redis if redis is configured
        if (parameter.has("redisHost") || parameter.has("redisPort")) {
            LOG.debug("Attaching redis host [" + redisHost + ":"+ redisPort+"]");
            topNTags.addSink(
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
        } else {
            LOG.debug("Redis was not configured, the redis sink for the Top N tags will not be added.");
        }

        /*
         *  Actually execute the streaming plan defined above.
         *  Everything up till this point was lazy execution.
         */
        env.execute("Twitter Top 10 hashtags");

    }

    /**
     * Maps incoming statuses as (ID, Text, Tags) with the tags as a space separated string.
     * This is done as a flatmap to be able to filter out bad values.
     */
    static class MapStatuses implements FlatMapFunction<String, Tuple3<Long, String, String>>{
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

                collector.collect(new Tuple3<>(statusID, statusText, hashTagString.toString()));

            } catch (JSONException | ArrayIndexOutOfBoundsException e) {/* Skip */}
        }
    }

    /**
     * Add the current time as timestamp to the status.
     * This is usually done in the source function, but the twitter connector has not done so. Hence the need to add the
     * timestamps manually.
     */
    static class AttachCurrentTime implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>>{

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 1000L);
        }

        @Override
        public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
            return System.currentTimeMillis();
        }
    }

    /**
     * FlatMaps all tags from incoming statuses as Tuple1<String>.
     */
    private static class FlatMapTags implements FlatMapFunction<Tuple3<Long, String, String>, Tuple2<String, Long>> {
        @Override
        public void flatMap(Tuple3<Long, String, String> value, Collector<Tuple2<String, Long>> out) throws Exception {
            for (String tag : value.f2.trim().split(" ")) {
                if (tag.equals(" ") || tag.equals("")) {
                    continue;
                }
                out.collect(new Tuple2<>(tag, 1L));
            }
        }
    }

    /**
     * Apply function for aggregating Tweet counts within a window.
     */
    static class CalcTopNTags implements AllWindowFunction<
            Tuple2<String, Long>,
            Tuple3<Integer, String, Long>,
            TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<String, Long>> values, Collector<Tuple3<Integer,
                String, Long>> out) throws Exception {
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

    /**
     * Takes a stream with a new Top N and the main stream of mapped tweets as input. Passes the text and ID of tweets
     * that have a top N tag attached.
     */
    static class FilterTopNCoFlatMap implements CoFlatMapFunction<
            Tuple3<Long, String, String>,
            Tuple3<Integer, String, Long>,
            Tuple2<Long, String>
            >{
        String[] topN;

        /**
         * Filters the messages with a top N tag attached.
         * Removes the tags as they are no longer needed.
         *
         * @param value (ID, Text, Tags)
         * @param out (ID, Text)
         */
        @Override
        public void flatMap1(Tuple3<Long, String, String> value, Collector<Tuple2<Long, String>> out) throws Exception {
            for (String tag : value.f2.split(" ")) {
                if (ArrayUtils.contains(topN, tag)) {
                    out.collect(new Tuple2<>(value.f0, value.f1));
                    break;
                }
            }
        }

        /**
         * Saves the new top N values in the filter function.
         *
         * @param value (top n position, tag, count)
         * @param out Nothing
         */
        @Override
        public void flatMap2(Tuple3<Integer, String, Long> value, Collector<Tuple2<Long, String>> out) throws Exception {
            if (topN == null) {
                topN = new String[n];
            }
            topN[value.f0 - 1] = value.f1;
        }
    }


}
