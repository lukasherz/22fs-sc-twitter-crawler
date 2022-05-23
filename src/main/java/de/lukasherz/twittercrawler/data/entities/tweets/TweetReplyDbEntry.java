package de.lukasherz.twittercrawler.data.entities.tweets;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetReplyDbEntry {
    private Long id;
    private long replyTweetId;
    private long repliedTweetId;

}