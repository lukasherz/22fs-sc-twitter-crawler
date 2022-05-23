package de.lukasherz.twittercrawler.data.entities.tweets;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetQuoteDbEntry {
    private Long id;
    private long quoteTweetId;
    private long quotedTweetId;

}