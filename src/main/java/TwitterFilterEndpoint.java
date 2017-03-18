import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;


public class TwitterFilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterFilterEndpoint.class);


    private final ArrayList<Long> followings;
    private final ArrayList<Location> locations;
    private ArrayList<String> languages;
    private ArrayList<String> trackTerms;


    TwitterFilterEndpoint() {
        this.languages = new ArrayList<>();
        this.trackTerms = new ArrayList<>();
        this.followings = new ArrayList<>();
        this.locations = new ArrayList<>();
    }

    public void addLanguage(String... language) {
        Collections.addAll(this.languages, language);
    }

    public void addTrackTerm(String... trackTerm) {
        Collections.addAll(this.trackTerms, trackTerm);
    }

    public void addLocation(Location... location) {
        Collections.addAll(this.locations, location);
    }

    public void addFollowing(Long... id) {
        Collections.addAll(this.followings, id);
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        if (languages.size() > 0) {
            LOG.info("Tracking languages: " + languages);
            endpoint.languages(languages);
        }
        if (followings.size() > 0) {
            LOG.info("Tracking IDs: " + followings);
            endpoint.followings(followings);
        }
        if (locations.size() > 0) {
            LOG.info("Tracking locations: " + locations);
            endpoint.locations(locations);
        }
        if (trackTerms.size() > 0) {
            LOG.info("Tracking terms: " + trackTerms);
            endpoint.trackTerms(trackTerms);
        }
        return endpoint;
    }

}
