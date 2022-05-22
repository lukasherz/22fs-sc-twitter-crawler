package de.lukasherz.twittercrawler.data.entities.tweets;

import de.lukasherz.twittercrawler.data.entities.users.User;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetReply {
    private long id;
    private Tweet replyTweet;
    private Tweet repliedTweet;

}