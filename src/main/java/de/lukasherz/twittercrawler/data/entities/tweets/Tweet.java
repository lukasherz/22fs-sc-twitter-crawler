package de.lukasherz.twittercrawler.data.entities.tweets;

import de.lukasherz.twittercrawler.data.entities.users.User;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.checkerframework.checker.index.qual.NonNegative;

import java.time.Instant;

@Builder
@Data
@AllArgsConstructor
public class Tweet {
    private long id;
    private User author;
    private String text;
    private Instant createdAt;
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

}