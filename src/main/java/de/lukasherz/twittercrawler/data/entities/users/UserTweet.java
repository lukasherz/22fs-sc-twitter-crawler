package de.lukasherz.twittercrawler.data.entities.users;

import de.lukasherz.twittercrawler.data.entities.tweets.Tweet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class UserTweet {
    private long id;
    private User user;
    private Tweet tweet;

}