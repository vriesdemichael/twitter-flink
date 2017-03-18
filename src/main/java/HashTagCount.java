/**
 * Model for saving A hashtag along with the count. Can be used as POJO within flink.
 */
public class HashTagCount implements Comparable<HashTagCount> {
    private int count;
    private String tag;

    public HashTagCount() {/* prerequisite for POJOs */}

    public HashTagCount(String tag, int count) {
        this.tag = tag;
        this.count = count;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "(" + tag + ", " + count + ")";
    }


    @Override
    public int compareTo(HashTagCount o) {
        return count - o.getCount();
    }
}
