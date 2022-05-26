package de.lukasherz.twittercrawler.crawler;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.GenericMultipleUsersLookupResponse;
import com.twitter.clientlib.model.MultiUserLookupResponse;
import com.twitter.clientlib.model.QuoteTweetLookupResponse;
import com.twitter.clientlib.model.TweetSearchResponse;
import com.twitter.clientlib.model.UsersFollowingLookupResponse;
import de.lukasherz.twittercrawler.crawler.Request.Priority;
import de.lukasherz.twittercrawler.crawler.requests.FollowsLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.HashtagSearchRequest;
import de.lukasherz.twittercrawler.crawler.requests.LikingUsersLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.QuotesLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.ReplyLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.RetweetsLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.UserLookupRequest;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.java.Log;
import org.apache.commons.lang3.NotImplementedException;

@Log
public class CrawlerHandler {

    private static CrawlerHandler instance;
    private final DatabaseManager dm = DatabaseManager.getInstance();
    private final HashSet<TwitterApi> apisBearer;
    private final RequestPriorityQueue<TweetSearchResponse> searchRecentTweetsQueue;
    private final RequestPriorityQueue<UsersFollowingLookupResponse> followingUsersQueue;
    private final RequestPriorityQueue<QuoteTweetLookupResponse> quotedTweetsQueue;
    private final RequestPriorityQueue<GenericMultipleUsersLookupResponse> retweetedTweetsQueue;
    private final RequestPriorityQueue<GenericMultipleUsersLookupResponse> likedTweetsQueue;
    private final RequestPriorityQueue<TweetSearchResponse> repliesTweetsQueue;
    private final RequestPriorityQueue<MultiUserLookupResponse> userLookupQueue;
    private QueuedTimer<TweetSearchResponse> searchRecentTweetsTimer;
    private QueuedTimer<UsersFollowingLookupResponse> followingUsersTimer;
    private QueuedTimer<QuoteTweetLookupResponse> quotedTweetsTimer;
    private QueuedTimer<GenericMultipleUsersLookupResponse> retweetedTweetsTimer;
    private QueuedTimer<GenericMultipleUsersLookupResponse> likedTweetsTimer;
    private QueuedTimer<TweetSearchResponse> repliesTweetsTimer;
    private QueuedTimer<MultiUserLookupResponse> userLookupTimer;

    private CrawlerHandler() {
        instance = this;

        Set<String> tokens = Set.of(
            "AAAAAAAAAAAAAAAAAAAAAAnqcQEAAAAAvCh8TM%2FpzS3pnvFL%2B9eraD5LJNo%3D1cAwKaKCB8hbbJAK4TtMX0YzyFv77CSiDHJYry5jrJ8V916ZFA"
//            "AAAAAAAAAAAAAAAAAAAAAKXrcQEAAAAAEFV%2BFRwZ%2Bs1yk6jWKjkqANvM0%2F0%3DpuMWkXJMkJuxsAMh94QlwlABMvdwffLWS3fD1HCksaR18ARaRb",
//            "AAAAAAAAAAAAAAAAAAAAAGLrcQEAAAAAXVPWeR34g3%2FVBOzrwJd54%2FH5oAo%3Df1nMWHK1c8YDgjVxC16ihfnRlJQ3KfnGwkKO72aSCAbwJdlY4f"
        );

        apisBearer = new HashSet<>();
        for (String token : tokens) {
            TwitterApi api = new TwitterApi();
            api.setTwitterCredentials(new TwitterCredentialsBearer(token));
            apisBearer.add(api);
        }

        searchRecentTweetsQueue = new RequestPriorityQueue<>(apisBearer);
        followingUsersQueue = new RequestPriorityQueue<>(apisBearer);
        quotedTweetsQueue = new RequestPriorityQueue<>(apisBearer);
        retweetedTweetsQueue = new RequestPriorityQueue<>(apisBearer);
        likedTweetsQueue = new RequestPriorityQueue<>(apisBearer);
        repliesTweetsQueue = new RequestPriorityQueue<>(apisBearer);
        userLookupQueue = new RequestPriorityQueue<>(apisBearer);

        searchRecentTweetsTimer = new QueuedTimer<>(searchRecentTweetsQueue, "searchRecentTweetsTimer");
        followingUsersTimer = new QueuedTimer<>(followingUsersQueue, "followingUsersTimer");
        quotedTweetsTimer = new QueuedTimer<>(quotedTweetsQueue, "quotedTweetsTimer");
        retweetedTweetsTimer = new QueuedTimer<>(retweetedTweetsQueue, "retweetedTweetsTimer");
        likedTweetsTimer = new QueuedTimer<>(likedTweetsQueue, "likedTweetsTimer");
        repliesTweetsTimer = new QueuedTimer<>(repliesTweetsQueue, "repliesTweetsTimer");
        userLookupTimer = new QueuedTimer<>(userLookupQueue, "userLookupTimer");
    }

    public static CrawlerHandler getInstance() {
        if (instance == null) {
            instance = new CrawlerHandler();
        }
        return instance;
    }

    public void startSchedulers() {
        searchRecentTweetsTimer.start();
        followingUsersTimer.start();
        quotedTweetsTimer.start();
        retweetedTweetsTimer.start();
        likedTweetsTimer.start();
        repliesTweetsTimer.start();
        userLookupTimer.start();

        new Timer().scheduleAtFixedRate(
            new TimerTask() {
                @Override public void run() {
                    System.out.println("\nCurrently queued requests: \n");
                    System.out.println("Search recent tweets: " + searchRecentTweetsQueue.size()
                        + " takes approximately " + (Math.ceil(searchRecentTweetsQueue.size() / 450.) * 15.)
                        + " minutes");
                    System.out.println("Following users: " + followingUsersQueue.size()
                        + " takes approximately " + (Math.ceil(followingUsersQueue.size() / 15.) * 15.) + " minutes");
                    System.out.println("Quoted tweets: " + quotedTweetsQueue.size()
                        + " takes approximately " + (Math.ceil(quotedTweetsQueue.size() / 75.) * 15.) + " minutes");
                    System.out.println("Retweeted tweets: " + retweetedTweetsQueue.size()
                        + " takes approximately " + (Math.ceil(retweetedTweetsQueue.size() / 75.) * 15.) + " minutes");
                    System.out.println("Liked tweets: " + likedTweetsQueue.size()
                        + " takes approximately " + (Math.ceil(likedTweetsQueue.size() / 75.) * 15.) + " minutes");
                    System.out.println("Replies tweets: " + repliesTweetsQueue.size()
                        + " takes approximately " + (Math.ceil(repliesTweetsQueue.size() / 450.) * 15.) + " minutes");
                    System.out.println("User lookup: " + userLookupQueue.size()
                        + " takes approximately " + (Math.ceil(userLookupQueue.size() / 300.) * 15.) + " minutes");

                    System.out.println("Total requests: " + (searchRecentTweetsQueue.size()
                        + followingUsersQueue.size() + quotedTweetsQueue.size() + retweetedTweetsQueue.size()
                        + likedTweetsQueue.size() + repliesTweetsQueue.size() + userLookupQueue.size()));

                    System.out.println(
                        "Total time: " + (
                            Math.ceil((searchRecentTweetsQueue.size() + repliesTweetsQueue.size()) / 450.) * 15.
                                + Math.ceil(followingUsersQueue.size() / 15.) * 15.
                                + Math.ceil(quotedTweetsQueue.size() / 75.) * 15.
                                + Math.ceil(retweetedTweetsQueue.size() / 75.) * 15.
                                + Math.ceil(likedTweetsQueue.size() / 75.) * 15.
                                + Math.ceil(userLookupQueue.size() / 300.) * 15.) + " minutes");
                    System.out.println("\n");
                }
            }, 0, 10000);
    }

    public void handleRateLimit(Request<?> request, Instant nextRequestAllowed) {
        log.info("Rate limit reached for " + request.getClass().getSimpleName() + ": " + nextRequestAllowed);

        if (request instanceof FollowsLookupRequest) {
            followingUsersQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            followingUsersQueue.offer((Request<UsersFollowingLookupResponse>) request);
        } else if (request instanceof HashtagSearchRequest) {
            searchRecentTweetsQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            searchRecentTweetsQueue.offer((Request<TweetSearchResponse>) request);
        } else if (request instanceof QuotesLookupRequest) {
            quotedTweetsQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            quotedTweetsQueue.offer((Request<QuoteTweetLookupResponse>) request);
        } else if (request instanceof RetweetsLookupRequest) {
            retweetedTweetsQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            retweetedTweetsQueue.offer((Request<GenericMultipleUsersLookupResponse>) request);
        } else if (request instanceof LikingUsersLookupRequest) {
            likedTweetsQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            likedTweetsQueue.offer((Request<GenericMultipleUsersLookupResponse>) request);
        } else if (request instanceof ReplyLookupRequest) {
            repliesTweetsQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            repliesTweetsQueue.offer((Request<TweetSearchResponse>) request);
        } else if (request instanceof UserLookupRequest) {
            userLookupQueue.setTimeForCurrentEntry(nextRequestAllowed);
            request.setPriority(Priority.HIGH);
            userLookupQueue.offer((Request<MultiUserLookupResponse>) request);
        } else {
            throw new NotImplementedException("Not implemented yet");
        }
    }

    public void addHashtagSearchToQuery(String hashtag, int count) {
        searchRecentTweetsQueue.offer(new HashtagSearchRequest(searchRecentTweetsQueue, hashtag, count));
    }

    public void addFollowsLookupToQuery(long userId) {
        //TODO: check if already computed
        followingUsersQueue.offer(new FollowsLookupRequest(followingUsersQueue, userId, 1000));
    }

    public void addQuotesLookupToQuery(long tweetId, int count) {
//        quotedTweetsQueue.offer(new QuotesLookupRequest(quotedTweetsQueue, tweetId, count));
    }

    public void addRetweetsLookupToQuery(long tweetId, int count) {
//        retweetedTweetsQueue.offer(new RetweetsLookupRequest(retweetedTweetsQueue, tweetId, count));
    }

    public void addLikingUsersLookupToQuery(long tweetId, int count) {
//        likedTweetsQueue.offer(new LikingUsersLookupRequest(likedTweetsQueue, tweetId, count));
    }

    public void addReplyLookupToQuery(long coversationId, int count) {
//        repliesTweetsQueue.offer(new ReplyLookupRequest(repliesTweetsQueue, coversationId, count));
    }

    public void addUsersTweetsLookupToQuery(long userId, int count) {
        // TODO
    }

    public void addUserLookupToQuery(long userId) {
        addUserLookupToQuery(Collections.singletonList(userId));
    }

    public void addUserLookupToQuery(List<Long> userIds) {
        addUserLookupToQuery(userIds, null);
    }

    public void addUserLookupToQuery(List<Long> userIds, Runnable runAfterExecution) {
        userIds.removeIf(id -> {
            try {
                return dm.existsUser(id);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return false;
        });

        if (userIds.isEmpty()) {
            return;
        }

        userLookupQueue.offer(new UserLookupRequest(userLookupQueue, userIds, runAfterExecution));
    }
}
