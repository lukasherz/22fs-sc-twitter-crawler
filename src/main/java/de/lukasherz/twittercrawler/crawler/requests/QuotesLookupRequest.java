package de.lukasherz.twittercrawler.crawler.requests;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.ContextAnnotation;
import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import com.twitter.clientlib.model.QuoteTweetLookupResponse;
import com.twitter.clientlib.model.Tweet;
import com.twitter.clientlib.model.TweetReferencedTweets.TypeEnum;
import de.lukasherz.twittercrawler.crawler.Request;
import de.lukasherz.twittercrawler.crawler.RequestPriorityQueue;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetQuoteDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetReferenceDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomainDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntityDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class QuotesLookupRequest extends Request<QuoteTweetLookupResponse> {

    private final RequestPriorityQueue<QuoteTweetLookupResponse> queue;
    private final long tweetId;
    private final int totalCountLeft;
//    private final String token;

    public QuotesLookupRequest(RequestPriorityQueue<QuoteTweetLookupResponse> queue, long tweetId, int totalCountLeft) {
        this.queue = queue;
        this.tweetId = tweetId;
        this.totalCountLeft = totalCountLeft;
//        this.token = null;
    }

//    private QuotesLookupRequest(RequestPriorityQueue<QuoteTweetLookupResponse> queue, long tweetId, int totalCountLeft,
//                                String token) {
//        this.queue = queue;
//        this.tweetId = tweetId;
//        this.totalCountLeft = totalCountLeft;
//        this.token = token;
//    }

    @Override protected QuoteTweetLookupResponse executeImpl() {
        try {
            QuoteTweetLookupResponse qtlr = queue.getNextApi().tweets().findTweetsThatQuoteATweet(
                String.valueOf(getTweetId()),
                getCountForThisRun(),
                Set.of(
                    "retweets",
                    "replies"
                ),
                Set.of(
                    "author_id",
                    "entities.mentions.username",
                    "in_reply_to_user_id",
                    "referenced_tweets.id",
                    "referenced_tweets.id.author_id",
                    "geo.place_id"
                ),
                Set.of(
                    "id",
                    "created_at",
                    "text",
                    "author_id",
                    "in_reply_to_user_id",
                    "referenced_tweets",
                    "geo",
                    "public_metrics",
                    "lang",
                    "context_annotations",
                    "conversation_id"
                ),
                Set.of(
                    "id",
                    "created_at",
                    "name",
                    "username",
                    "verified",
                    "profile_image_url",
                    "location",
                    "url",
                    "description"
                ),
                Set.of(
                    "media_key",
                    "type",
                    "url"
                ),
                Set.of(
                    "id",
                    "name",
                    "country_code",
                    "full_name",
                    "country",
                    "geo"
                ),
                null
            );

            if (getCountLeft() > 0
                && (qtlr.getMeta() != null && qtlr.getMeta().getResultCount() != null
                && qtlr.getMeta().getResultCount() == getCountForThisRun())) {
                queue.offer(new QuotesLookupRequest(
                    queue,
                    getTweetId(),
                    getCountLeft()
//                    qtlr.getMeta().getNextToken()
                ));
            }

            return qtlr;
        } catch (ApiException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void processQuotesLookupRequestResult(QuoteTweetLookupResponse qtlr) {
        if (qtlr == null) {
            return;
        }

        DatabaseManager dm = DatabaseManager.getInstance();

        if (qtlr.getIncludes() != null && qtlr.getIncludes().getUsers() != null) {
            try {
                dm.insertUsers(qtlr.getIncludes().getUsers().stream().map(UserDbEntry::parse).toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (qtlr.getData() != null) {
            try {
                dm.insertTweets(qtlr.getData().stream()
                    .filter(tweet -> tweet.getReferencedTweets() != null)
                    .filter(t -> t.getReferencedTweets().stream().anyMatch(r -> r.getType() == TypeEnum.QUOTED))
                    .map(m -> TweetDbEntry.parse(m, null))
                    .toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                dm.insertTweetQuotes(qtlr.getData().stream()
                    .filter(tweet -> tweet.getReferencedTweets() != null)
                    .filter(t -> t.getReferencedTweets().stream().anyMatch(r -> r.getType() == TypeEnum.QUOTED))
                    .map(t -> TweetQuoteDbEntry.builder()
                        .quotedTweetId(Long.parseLong(t.getReferencedTweets().stream()
                            .filter(r -> r.getType() == TypeEnum.QUOTED)
                            .findFirst().get().getId()))
                        .quoteTweetId(Long.parseLong(t.getId()))
                        .build())
                    .toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // retweets
//            try {
//                dm.insertTweetRetweets(qtlr.getData().stream()
//                    .filter(tweet -> tweet.getReferencedTweets() != null)
//                    .filter(t -> t.getReferencedTweets().stream().anyMatch(r -> r.getType() == TypeEnum.RETWEETED))
//                    .map(t -> TweetRetweetDbEntry.builder()
//                        .userId(Long.parseLong(t.getAuthorId()))
//                        .tweetId(Long.parseLong(t.getReferencedTweets().stream()
//                            .filter(r -> r.getType() == TypeEnum.RETWEETED)
//                            .findFirst().get().getId()))
//                        .build())
//                    .toList());
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
        }

        try {
            dm.insertContextAnnotationDomains(qtlr.getData().stream()
                .map(Tweet::getContextAnnotations)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(ContextAnnotation::getDomain)
                .map(ContextAnnotationDomainDbEntry::parse)
                .toList());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            dm.insertContextAnnotationEntities(qtlr.getData().stream()
                .map(Tweet::getContextAnnotations)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(ContextAnnotation::getEntity)
                .map(ContextAnnotationEntityDbEntry::parse)
                .toList());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            dm.insertContextAnnotations(qtlr.getData().stream()
                .map(Tweet::getContextAnnotations)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(ContextAnnotationDbEntry::parse)
                .toList());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (Tweet tweet : qtlr.getData()) {
            if (tweet.getContextAnnotations() == null) {
                break;
            }

            try {
                dm.insertTweetContextAnnotations(
                    Long.parseLong(tweet.getId()),
                    tweet.getContextAnnotations().stream()
                        .map(ContextAnnotation::getDomain)
                        .map(ContextAnnotationDomainFields::getId)
                        .map(Long::parseLong)
                        .collect(Collectors.toList()),
                    tweet.getContextAnnotations().stream()
                        .map(ContextAnnotation::getEntity)
                        .map(ContextAnnotationEntityFields::getId)
                        .map(Long::parseLong)
                        .collect(Collectors.toList())
                );
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            dm.insertTweetReferences(qtlr.getData().stream()
                .filter(t -> t.getReferencedTweets() != null)
                .flatMap(t -> TweetReferenceDbEntry.parse(t).stream())
                .collect(Collectors.toList())
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override protected QuoteTweetLookupResponse executeAndProcessImpl() {
        QuoteTweetLookupResponse qtlr = executeImpl();
        processQuotesLookupRequestResult(qtlr);
        return qtlr;
    }

    public long getTweetId() {
        return tweetId;
    }

    private int getCountForThisRun() {
        return Math.max(10, Math.min(totalCountLeft, 100));
    }

    private int getCountLeft() {
        return Math.max(0, totalCountLeft - Math.min(totalCountLeft, 100));
    }
}
