package de.lukasherz.twittercrawler.data.entities.tweets;

import com.twitter.clientlib.model.Tweet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.checkerframework.checker.index.qual.NonNegative;

import java.time.Instant;

@Builder
@Data
@AllArgsConstructor
public class TweetDbEntry {
    private long id;
    private long authorId;
    private String text;
    private Instant createdAt;
    private String searchQuery;
    @NonNegative
    private int metricsRetweetCount;
    @NonNegative
    private int metricsLikeCount;
    @NonNegative
    private int metricsReplyCount;
    @NonNegative
    private int metricsQuoteCount;
    private String lang;
    private String geo;

    public static TweetDbEntry parse(Tweet tweet, String searchQuery) {
        return TweetDbEntry.builder()
                .id(Long.parseLong(tweet.getId()))
                .authorId(Long.parseLong(tweet.getAuthorId()))
                .text(tweet.getText())
                .createdAt(tweet.getCreatedAt().toInstant())
                .searchQuery(searchQuery)
                .metricsRetweetCount(tweet.getPublicMetrics().getRetweetCount())
                .metricsLikeCount(tweet.getPublicMetrics().getLikeCount())
                .metricsReplyCount(tweet.getPublicMetrics().getReplyCount())
                .metricsQuoteCount(tweet.getPublicMetrics().getQuoteCount())
                .lang(tweet.getLang())
                .geo(tweet.getGeo() != null ? tweet.getGeo().toString() : null)
                .build();
    }

}