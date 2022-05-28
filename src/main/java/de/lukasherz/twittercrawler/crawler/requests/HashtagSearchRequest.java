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
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomainDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntityDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.flogger.Flogger;
import org.checkerframework.checker.index.qual.Positive;

@Flogger
public class HashtagSearchRequest extends Request<TweetSearchResponse> {

    private final RequestPriorityQueue<TweetSearchResponse> queue;
    private final String hashtag;
    private final int totalCountLeft;
    private final String token;

    /**
     * Returns the next request to be crawled.
     *
     * @param queue   the queue to add the results to
     * @param hashtag including the "#" upfront
     * @param count   the total number of tweets to be crawled
     */
    public HashtagSearchRequest(RequestPriorityQueue<TweetSearchResponse> queue, String hashtag, @Positive int count) {
        this.queue = queue;
        this.hashtag = hashtag;
        this.totalCountLeft = count;
        this.token = null;
    }

    /**
     * Returns the next request to be crawled.
     *
     * @param queue          the queue to add the results to
     * @param hashtag        including the "#" upfront
     * @param totalCountLeft the total number of tweets to be crawled
     * @param token          the token to be used for the next request
     */
    private HashtagSearchRequest(RequestPriorityQueue<TweetSearchResponse> queue,
                                 String hashtag,
                                 int totalCountLeft,
                                 String token) {
        this.queue = queue;
        this.hashtag = hashtag;
        this.totalCountLeft = totalCountLeft;
        this.token = token;
    }

    private void processHashtagSearchRequestResult(TweetSearchResponse tsr) {
        if (tsr == null) {
            return;
        }

        DatabaseManager dm = DatabaseManager.getInstance();
        CrawlerHandler ch = CrawlerHandler.getInstance();

        try {
            if (tsr.getIncludes() != null && tsr.getIncludes().getUsers() != null) {
                dm.insertUsers(tsr.getIncludes().getUsers().stream().map(UserDbEntry::parse).toList());

                if (tsr.getData() != null) {
                    tsr.getIncludes().getUsers().stream()
                        .map(User::getId)
                        .filter(id -> tsr.getData().stream()
                            .filter(t -> t.getAuthorId() != null)
                            .anyMatch(t -> t.getAuthorId().equalsIgnoreCase(id)))
                        .map(Long::parseLong)
                        .forEach(ch::addFollowsLookupToQuery);
                }
            }

            if (tsr.getData() != null) {
                dm.insertTweets(tsr.getData().stream().map(m -> TweetDbEntry.parse(m, getQuery())).toList());

                dm.insertContextAnnotationDomains(tsr.getData().stream()
                    .map(Tweet::getContextAnnotations)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .map(ContextAnnotation::getDomain)
                    .map(ContextAnnotationDomainDbEntry::parse)
                    .toList());

                dm.insertContextAnnotationEntities(tsr.getData().stream()
                    .map(Tweet::getContextAnnotations)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .map(ContextAnnotation::getEntity)
                    .map(ContextAnnotationEntityDbEntry::parse)
                    .toList());

                dm.insertContextAnnotations(tsr.getData().stream()
                    .map(Tweet::getContextAnnotations)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .map(ContextAnnotationDbEntry::parse)
                    .toList());

                for (Tweet tweet : tsr.getData()) {
                    if (tweet.getContextAnnotations() == null) {
                        break;
                    }

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
                }

                // no references because they are excluded in query
//                dm.insertTweetReferences(tsr.getData().stream()
//                    .filter(t -> t.getReferencedTweets() != null)
//                    .flatMap(t -> TweetReferenceDbEntry.parse(t).stream())
//                    .collect(Collectors.toList())
//                );

            }
        } catch (SQLException e) {
            log.atSevere().withCause(e).log("Failed to insert HashtagTweetsResponse to database");
        }
    }

    @Override
    protected TweetSearchResponse executeImpl() {
        log.atInfo().log("Executing search for query: \"%s\" with %d tweets and %d tweets left",
            getQuery(), getCountForThisRun(), getCountLeft());

        try {
            TweetSearchResponse tsr = queue.getNextApi().tweets().tweetsRecentSearch(
                getQuery(),
                null,
                //OffsetDateTime.now().minus(1, ChronoUnit.DAYS),
                null,
                null,
                null,
                getCountForThisRun(),
                null,
                null,
                token,
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
                queue.offer(new HashtagSearchRequest(queue,
                    hashtag,
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
            } else {
                log.atSevere().withCause(e).log("Could not get rate limit information from response headers.");
            }
        }

        return null;
    }

    @Override protected TweetSearchResponse executeAndProcessImpl() {
        TweetSearchResponse tsr = executeImpl();
        processHashtagSearchRequestResult(tsr);
        return tsr;
    }

    public String getQuery() {
        return hashtag + " -is:retweet -is:reply -is:quote lang:en";
    }

    private int getCountForThisRun() {
        return Math.max(10, Math.min(totalCountLeft, 100));
    }

    private int getCountLeft() {
        return Math.max(0, totalCountLeft - Math.min(totalCountLeft, 100));
    }
}
