package de.lukasherz.twittercrawler.data.entities.tweets.contextannotation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetContextAnnotationDbEntry {
    private Long id;
    private long tweetId;
    private long contextAnnotationId;
}