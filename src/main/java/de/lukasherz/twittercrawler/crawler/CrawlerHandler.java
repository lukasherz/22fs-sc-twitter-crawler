package de.lukasherz.twittercrawler.crawler;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.TweetSearchResponse;
import com.twitter.clientlib.model.UsersFollowingLookupResponse;
import de.lukasherz.twittercrawler.TwitterCrawler;
import de.lukasherz.twittercrawler.crawler.Request.Priority;
import de.lukasherz.twittercrawler.crawler.requests.FollowsLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.HashtagSearchRequest;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
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
    private QueuedTimer<TweetSearchResponse> searchRecentTweetsTimer;
    private QueuedTimer<UsersFollowingLookupResponse> followingUsersTimer;

    private CrawlerHandler() {
        instance = this;

        apisBearer = new HashSet<>();
        for (String token : Collections.singleton(TwitterCrawler.TOKEN)) {
            TwitterApi api = new TwitterApi();
            api.setTwitterCredentials(new TwitterCredentialsBearer(token));
            apisBearer.add(api);
        }

        searchRecentTweetsQueue = new RequestPriorityQueue<>(apisBearer);
        followingUsersQueue = new RequestPriorityQueue<>(apisBearer);

        searchRecentTweetsTimer = new QueuedTimer<>(searchRecentTweetsQueue, "searchRecentTweetsTimer");
        followingUsersTimer = new QueuedTimer<>(followingUsersQueue, "followingUsersTimer");
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

        new Timer().scheduleAtFixedRate(
            new TimerTask() {
                @Override public void run() {
                    System.out.println("\nCurrently queued requests: \n");
                    System.out.println("Search recent tweets: " + searchRecentTweetsQueue.size()
                        + " takes approximately " + (Math.ceil(searchRecentTweetsQueue.size() / 450.) * 15.)
                        + " minutes");
                    System.out.println("Following users: " + followingUsersQueue.size()
                        + " takes approximately " + (Math.ceil(followingUsersQueue.size() / 15.) * 15.) + " minutes");

                    System.out.println("Total requests: " + (searchRecentTweetsQueue.size()
                        + followingUsersQueue.size()));

                    System.out.println(
                        "Total time: " + (
                            Math.ceil(searchRecentTweetsQueue.size() / 450.) * 15.
                                + Math.ceil(followingUsersQueue.size() / 15.) * 15.) + " minutes");
                    System.out.println(
                        "Total time (worst case): " + (
                            Math.ceil(searchRecentTweetsQueue.size() * 1100 / 450.) * 15.
                                + Math.ceil(followingUsersQueue.size() * 10000 / 15.) * 15.) + " minutes");
                    System.out.println(
                        "Total time (worst case): " + (
                            Math.ceil(searchRecentTweetsQueue.size() * 1100 / 450.) * 15.
                                + Math.ceil(followingUsersQueue.size() * 10000 / 15.) * 15.) / 60. + " hours");
                    System.out.println(
                        "Total time (worst case): " + (
                            Math.ceil(searchRecentTweetsQueue.size() * 1100 / 450.) * 15.
                                + Math.ceil(followingUsersQueue.size() * 10000 / 15.) * 15.) / 60. / 24. + " days");
                    System.out.println("\n");
                }
            }, 0, 10000);
    }

    public void handleRateLimit(Request<?> request, Instant nextRequestAllowed) {
        log.info("Rate limit reached for " + request.getClass().getSimpleName() + ": " + nextRequestAllowed);
        request.setPriority(Priority.HIGHEST);

        if (request instanceof FollowsLookupRequest) {
            followingUsersQueue.setTimeForCurrentEntry(nextRequestAllowed);
            followingUsersQueue.offer((Request<UsersFollowingLookupResponse>) request);
        } else if (request instanceof HashtagSearchRequest) {
            searchRecentTweetsQueue.setTimeForCurrentEntry(nextRequestAllowed);
            searchRecentTweetsQueue.offer((Request<TweetSearchResponse>) request);
        } else {
            throw new NotImplementedException("Not implemented yet");
        }
    }

    public void addHashtagSearchToQuery(String hashtag, int count) {
        searchRecentTweetsQueue.offer(new HashtagSearchRequest(searchRecentTweetsQueue, hashtag, count));
    }

    public void addFollowsLookupToQuery(long userId) {
        //TODO: check if already computed
        followingUsersQueue.offer(new FollowsLookupRequest(followingUsersQueue, userId, 10000));
    }
}
