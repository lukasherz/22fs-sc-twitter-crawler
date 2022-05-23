package de.lukasherz.twittercrawler.data.entities.tweets;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetRetweetDbEntry {
    private Long id;
    private long userId;
    private long tweetId;

}