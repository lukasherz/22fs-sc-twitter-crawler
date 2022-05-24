package de.lukasherz.twittercrawler.crawler.requests;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.GenericMultipleUsersLookupResponse;
import com.twitter.clientlib.model.User;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import de.lukasherz.twittercrawler.crawler.Request;
import de.lukasherz.twittercrawler.crawler.RequestPriorityQueue;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetRetweetDbEntry;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class RetweetsLookupRequest extends Request<GenericMultipleUsersLookupResponse> {

    private final RequestPriorityQueue<GenericMultipleUsersLookupResponse> queue;
    private final long tweetId;
    private final int totalCountLeft;
    private final String token;

    public RetweetsLookupRequest(RequestPriorityQueue<GenericMultipleUsersLookupResponse> queue, long tweetId,
                                 int totalCountLeft) {
        this.queue = queue;
        this.tweetId = tweetId;
        this.totalCountLeft = totalCountLeft;
        this.token = null;
    }

    private RetweetsLookupRequest(RequestPriorityQueue<GenericMultipleUsersLookupResponse> queue, long tweetId,
                                  int totalCountLeft,
                                  String token) {
        this.queue = queue;
        this.tweetId = tweetId;
        this.totalCountLeft = totalCountLeft;
        this.token = token;
    }

    @Override protected GenericMultipleUsersLookupResponse executeImpl() {
        try {
            GenericMultipleUsersLookupResponse gmulr = queue.getNextApi().users().tweetsIdRetweetingUsers(
                String.valueOf(getTweetId()),
                getCountForThisRun(),
                token
            );

            if (getCountLeft() > 0
                && (gmulr.getMeta() != null && gmulr.getMeta().getResultCount() != null
                && gmulr.getMeta().getResultCount() == getCountForThisRun())) {
                queue.offer(new RetweetsLookupRequest(
                    queue,
                    tweetId,
                    getCountLeft(),
                    gmulr.getMeta() != null ? gmulr.getMeta().getNextToken() : null
                ));
            }

            return gmulr;
        } catch (ApiException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override protected void runAfterExecutionImpl(GenericMultipleUsersLookupResponse result) {
        CrawlerHandler ch = CrawlerHandler.getInstance();
        DatabaseManager dm = DatabaseManager.getInstance();

        if (result != null && result.getData() != null) {
            ch.addUserLookupToQuery(
                result.getData().stream()
                    .map(User::getId)
                    .map(Long::parseLong)
                    .collect(Collectors.toList()),
                () -> {
                    try {
                        dm.insertTweetRetweets(
                            result.getData().stream()
                                .map(t -> TweetRetweetDbEntry.builder()
                                    .userId(Long.parseLong(t.getId()))
                                    .tweetId(getTweetId())
                                    .build())
                                .toList()
                        );
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
        }
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
