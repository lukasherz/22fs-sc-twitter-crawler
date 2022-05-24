package de.lukasherz.twittercrawler.crawler.requests;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.ContextAnnotation;
import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import com.twitter.clientlib.model.Tweet;
import com.twitter.clientlib.model.TweetSearchResponse;
import com.twitter.clientlib.model.User;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import de.lukasherz.twittercrawler.crawler.Request;
import de.lukasherz.twittercrawler.crawler.RequestPriorityQueue;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetReferenceDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomainDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntityDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import java.sql.Date;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.java.Log;

@Log
public class ReplyLookupRequest extends Request<TweetSearchResponse> {

    private final RequestPriorityQueue<TweetSearchResponse> queue;
    private final long conversationId;
    private final int totalCountLeft;
    private final String token;

    public ReplyLookupRequest(RequestPriorityQueue<TweetSearchResponse> queue, long conversationId,
                              int totalCountLeft) {
        this.queue = queue;
        this.conversationId = conversationId;
        this.totalCountLeft = totalCountLeft;
        this.token = null;
    }

    private ReplyLookupRequest(RequestPriorityQueue<TweetSearchResponse> queue, long conversationId, int totalCountLeft,
                               String token) {
        this.queue = queue;
        this.conversationId = conversationId;
        this.totalCountLeft = totalCountLeft;
        this.token = token;
    }

    @Override protected TweetSearchResponse executeImpl() {
        try {
            TweetSearchResponse tsr = queue.getNextApi().tweets().tweetsRecentSearch(
                getQuery(),
                null,
                null,
                null,
                null,
                getCountForThisRun(),
                "relevancy",
                token,
                null,
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
                && (tsr.getMeta() != null && tsr.getMeta().getResultCount() != null
                && tsr.getMeta().getResultCount() == getCountForThisRun())) {
                queue.offer(new ReplyLookupRequest(queue,
                    getConversationId(),
                    getCountLeft(),
                    tsr.getMeta() != null ? tsr.getMeta().getNextToken() : null));
            }

            return tsr;
        } catch (ApiException e) {
            if (e.getResponseHeaders() != null && e.getResponseHeaders().containsKey("x-rate-limit-remaining")) {
                CrawlerHandler.getInstance().handleRateLimit(
                    this,
                    Instant.ofEpochSecond(Long.parseLong(e.getResponseHeaders().get("x-rate-limit-reset").get(0)))
                );
                log.info("Rate limit reached (" + this.getClass().getName() + "), waiting for " + Date.from(Instant.ofEpochSecond(Long.parseLong(e.getResponseHeaders().get("x-rate-limit-reset").get(0)))));
            } else {
                log.severe("Could not get rate limit information from response headers. " + this.getClass().getName());
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override public TweetSearchResponse executeAndProcessImpl() {
        TweetSearchResponse tsr = executeImpl();
        processReplyLookupRequestResult(tsr);
        return tsr;
    }

    private void processReplyLookupRequestResult(TweetSearchResponse tsr) {
        if (tsr == null) {
            return;
        }

        DatabaseManager dm = DatabaseManager.getInstance();
        CrawlerHandler ch = CrawlerHandler.getInstance();

        if (tsr.getIncludes() != null && tsr.getIncludes().getUsers() != null) {
            try {
                dm.insertUsers(tsr.getIncludes().getUsers().stream().map(UserDbEntry::parse).toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }

            tsr.getIncludes().getUsers().stream()
                .map(User::getId)
                .map(Long::parseLong)
                .forEach(ch::addFollowsLookupToQuery);
        }

        if (tsr.getData() != null) {
            try {
                dm.insertTweets(tsr.getData().stream().map(m -> TweetDbEntry.parse(m, getQuery())).toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (tsr.getData() == null) return;

        try {
            dm.insertContextAnnotationDomains(tsr.getData().stream()
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
            dm.insertContextAnnotationEntities(tsr.getData().stream()
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
            dm.insertContextAnnotations(tsr.getData().stream()
                .map(Tweet::getContextAnnotations)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(ContextAnnotationDbEntry::parse)
                .toList());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (Tweet tweet : tsr.getData()) {
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
            dm.insertTweetReferences(tsr.getData().stream()
                .filter(t -> t.getReferencedTweets() != null)
                .flatMap(t -> TweetReferenceDbEntry.parse(t).stream())
                .collect(Collectors.toList())
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long getConversationId() {
        return conversationId;
    }

    public String getQuery() {
        return "conversation_id:" + getConversationId() + " is:reply lang:en";
    }

    private int getCountForThisRun() {
        return Math.max(10, Math.min(totalCountLeft, 100));
    }

    private int getCountLeft() {
        return Math.max(0, totalCountLeft - Math.min(totalCountLeft, 100));
    }
}
