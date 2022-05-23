package de.lukasherz.twittercrawler.data.entities.tweets;

import com.google.common.collect.Lists;
import com.twitter.clientlib.model.Tweet;
import com.twitter.clientlib.model.TweetReferencedTweets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Builder
@Data
@AllArgsConstructor
public class TweetReferenceDbEntry {
    private Long id;
    private long tweetId;
    private long referencedTweetId;
    private String referenceType;

    public static TweetReferenceDbEntry parse(long tweetId, TweetReferencedTweets tweetReferencedTweets) {
        return TweetReferenceDbEntry.builder()
                .tweetId(tweetId)
                .referencedTweetId(Long.parseLong(tweetReferencedTweets.getId()))
                .referenceType(tweetReferencedTweets.getType().getValue())
                .build();
    }

    public static List<TweetReferenceDbEntry> parse(Tweet tweet) {
        if (tweet.getReferencedTweets() == null) {
            return List.of();
        }

        return tweet.getReferencedTweets().stream()
                .map(referencedTweet -> TweetReferenceDbEntry.builder()
                        .tweetId(Long.parseLong(tweet.getId()))
                        .referencedTweetId(Long.parseLong(referencedTweet.getId()))
                        .referenceType(referencedTweet.getType().getValue())
                        .build())
                .collect(Collectors.toList());
    }
}