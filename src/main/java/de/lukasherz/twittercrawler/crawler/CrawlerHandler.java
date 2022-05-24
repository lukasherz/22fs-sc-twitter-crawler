package de.lukasherz.twittercrawler.crawler;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.GenericMultipleUsersLookupResponse;
import com.twitter.clientlib.model.QuoteTweetLookupResponse;
import com.twitter.clientlib.model.TweetSearchResponse;
import com.twitter.clientlib.model.UsersFollowingLookupResponse;
import de.lukasherz.twittercrawler.crawler.requests.FollowsLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.HashtagSearchRequest;
import de.lukasherz.twittercrawler.crawler.requests.LikingUsersLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.QuotesLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.ReplyLookupRequest;
import de.lukasherz.twittercrawler.crawler.requests.RetweetsLookupRequest;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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

    public CrawlerHandler() {
        instance = this;

        Set<String> tokens = Set.of(
            "AAAAAAAAAAAAAAAAAAAAAAnqcQEAAAAAvCh8TM%2FpzS3pnvFL%2B9eraD5LJNo%3D1cAwKaKCB8hbbJAK4TtMX0YzyFv77CSiDHJYry5jrJ8V916ZFA"
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
    }

    public static CrawlerHandler getInstance() {
        return instance;
    }

    public void startSchedulers() {
        throw new NotImplementedException("Not implemented yet");
    }

    public void addHashtagSearchToQuery(String hashtag, int count) {
        searchRecentTweetsQueue.offer(new HashtagSearchRequest(searchRecentTweetsQueue, hashtag, count));
    }

    public void addFollowsLookupToQuery(long userId) {
        //TODO: check if already computed
        followingUsersQueue.offer(new FollowsLookupRequest(followingUsersQueue, userId, 1000));
    }

    public void addQuotesLookupToQuery(long tweetId, int count) {
        quotedTweetsQueue.offer(new QuotesLookupRequest(quotedTweetsQueue, tweetId, count));
    }

    public void addRetweetsLookupToQuery(long tweetId, int count) {
        retweetedTweetsQueue.offer(new RetweetsLookupRequest(retweetedTweetsQueue, tweetId, count));
    }

    public void addLikingUsersLookupToQuery(long tweetId, int count) {
        likedTweetsQueue.offer(new LikingUsersLookupRequest(likedTweetsQueue, tweetId, count));
    }

    public void addReplyLookupToQuery(long coversationId, int count) {
        repliesTweetsQueue.offer(new ReplyLookupRequest(repliesTweetsQueue, coversationId, count));
    }

    public void addUsersTweetsLookupToQuery(long userId, int count) {
        // TODO
    }

    public void addUserLookupToQuery(long userId) {
        addUserLookupToQuery(Collections.singleton(userId));
    }

    public void addUserLookupToQuery(Collection<Long> userIds) {
        addUserLookupToQuery(userIds, null);
    }

    public void addUserLookupToQuery(Collection<Long> userIds, Runnable runAfterExecution) {
        throw new NotImplementedException("Not implemented yet");
    }
}
