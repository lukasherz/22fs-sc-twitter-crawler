package de.lukasherz.twittercrawler.data.entities.tweets.contextannotation;

import de.lukasherz.twittercrawler.data.entities.tweets.Tweet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class TweetContextAnnotation {
    private long id;
    private Tweet tweet;
    private ContextAnnotation contextAnnotation;

}