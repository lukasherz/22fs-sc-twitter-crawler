package de.lukasherz.twittercrawler.data.entities.tweets;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetReference {
    private long id;
    private Tweet tweet;
    private Tweet referencedTweet;
    private String referenceType;

}