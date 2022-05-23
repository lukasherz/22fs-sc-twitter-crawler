package de.lukasherz.twittercrawler.crawler;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetReferenceDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomainDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntityDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import lombok.extern.java.Log;
import org.checkerframework.checker.index.qual.Positive;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Log
public class CrawlerHandler {
    private final DatabaseManager dm = DatabaseManager.getInstance();
    private final HashSet<TwitterApi> apis;
    private final RequestPriorityQueue<TweetSearchResponse> searchRecentTweetsQueue;

    public CrawlerHandler() {
        Set<String> tokens = Set.of(
                "AAAAAAAAAAAAAAAAAAAAAAnqcQEAAAAAvCh8TM%2FpzS3pnvFL%2B9eraD5LJNo%3D1cAwKaKCB8hbbJAK4TtMX0YzyFv77CSiDHJYry5jrJ8V916ZFA"
        );

        apis = new HashSet<>();
        for (String token : tokens) {
            TwitterApi api = new TwitterApi();
            api.setTwitterCredentials(new TwitterCredentialsBearer(token));
            apis.add(api);
        }

        searchRecentTweetsQueue = new RequestPriorityQueue<>(apis);
    }

    public void start() {
        search("#trump -is:retweet -is:reply -is:quote lang:en", 100);
    }


    public void search(String query, @Positive int count) {
        Request<TweetSearchResponse> search = new Request<>() {
            @Override
            public TweetSearchResponse execute() {
                try {
                    return searchRecentTweetsQueue.getNextApi().tweets().tweetsRecentSearch(
                            query,
                            null,
                            OffsetDateTime.now().minus(1, ChronoUnit.DAYS),
                            null,
                            null,
                            count,
                            "relevancy",
                            null,
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
                                    "context_annotations"
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
                } catch (ApiException e) {
                    e.printStackTrace();
                }

                return null;
            }
        };

        TweetSearchResponse tsr = search.execute();
        if (tsr != null) {
            int i = 0;

            if (tsr.getIncludes().getUsers() != null) {
                try {
                    dm.insertUsers(tsr.getIncludes().getUsers().stream().map(UserDbEntry::parse).toList());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (tsr.getData() != null) {
                try {
                    dm.insertTweets(tsr.getData().stream().map(m -> TweetDbEntry.parse(m, query)).toList());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

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

//
//            for (Tweet t : tsr.getData()) {
////                UserDbEntry userDbEntry = UserDbEntry.parse(tsr.getIncludes().getUsers().get(i++));
////                TweetDbEntry tweetDbEntry = TweetDbEntry.parse(t, query);
//
//                ArrayList<ContextAnnotationDbEntry> contextAnnotationDbEntries = new ArrayList<>();
//                ArrayList<ContextAnnotationDomainDbEntry> contextAnnotationDomainDbEntries = new ArrayList<>();
//                ArrayList<ContextAnnotationEntityDbEntry> contextAnnotationEntityDbEntries = new ArrayList<>();
//                if (t.getContextAnnotations() != null) {
//                    for (ContextAnnotation ca : t.getContextAnnotations()) {
//                        contextAnnotationDbEntries.add(ContextAnnotationDbEntry.parse(ca));
//                        contextAnnotationDomainDbEntries.add(ContextAnnotationDomainDbEntry.parse(ca.getDomain()));
//                        contextAnnotationEntityDbEntries.add(ContextAnnotationEntityDbEntry.parse(ca.getEntity()));
//                    }
//                }
//
//                ArrayList<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
//                if (t.getReferencedTweets() != null) {
//                    for (TweetReferencedTweets rt : t.getReferencedTweets()) {
//                        tweetReferenceDbEntries.add(TweetReferenceDbEntry.parse(Long.parseLong(t.getId()), rt));
//                    }
//                }
//
//                try {
////                    dm.insertUser(userDbEntry);
////                    dm.insertTweet(tweetDbEntry);
//                    for (ContextAnnotationDomainDbEntry entry : contextAnnotationDomainDbEntries) {
//                        dm.insertContextAnnotationDomain(entry);
//                    }
//                    for (ContextAnnotationEntityDbEntry entry : contextAnnotationEntityDbEntries) {
//                        dm.insertContextAnnotationEntity(entry);
//                    }
//                    for (ContextAnnotationDbEntry entry : contextAnnotationDbEntries) {
//                        dm.insertContextAnnotation(entry);
//                    }
//                    for (ContextAnnotationDbEntry entry : contextAnnotationDbEntries) {
//                        Optional<ContextAnnotationDbEntry> oe = dm.getContextAnnotation(entry.getContextAnnotationEntityId());
//                        if (oe.isPresent()) {
//                            dm.insertTweetContextAnnotation(TweetContextAnnotationDbEntry.builder()
//                                    .contextAnnotationId(oe.get().getId())
//                                    .tweetId(Long.parseLong(t.getId()))
//                                    .build());
//                        } else {
//                            log.info("Could not find context annotation entity with id " + entry.getContextAnnotationEntityId() + " for tweet " + t.getId() + "\n" +
//                                    "Context annotation: " + contextAnnotationEntityDbEntries.stream().filter(p -> p.getId() == entry.getContextAnnotationEntityId()).findFirst().get());
//                        }
//                    }
//                    for (TweetReferenceDbEntry entry : tweetReferenceDbEntries) {
//                        dm.insertTweetReference(entry);
//                    }
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
        }
    }
}
