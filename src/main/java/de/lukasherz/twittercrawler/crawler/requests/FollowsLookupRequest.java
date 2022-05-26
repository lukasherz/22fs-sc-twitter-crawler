package de.lukasherz.twittercrawler.crawler.requests;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.User;
import com.twitter.clientlib.model.UsersFollowingLookupResponse;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import de.lukasherz.twittercrawler.crawler.Request;
import de.lukasherz.twittercrawler.crawler.RequestPriorityQueue;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import java.sql.Date;
import java.sql.SQLException;
import java.time.Instant;
import java.util.stream.Collectors;
import lombok.extern.java.Log;

@Log
public class FollowsLookupRequest extends Request<UsersFollowingLookupResponse> {

    private final RequestPriorityQueue<UsersFollowingLookupResponse> queue;
    private final long userId;
    private final int totalCountLeft;
    private final String token;

    public FollowsLookupRequest(RequestPriorityQueue<UsersFollowingLookupResponse> queue, long userId,
                                int totalCountLeft) {
        this.queue = queue;
        this.userId = userId;
        this.totalCountLeft = totalCountLeft;
        this.token = null;
    }

    public FollowsLookupRequest(RequestPriorityQueue<UsersFollowingLookupResponse> queue, long userId) {
        this.queue = queue;
        this.userId = userId;
        this.totalCountLeft = Integer.MAX_VALUE;
        this.token = null;
    }

    private FollowsLookupRequest(RequestPriorityQueue<UsersFollowingLookupResponse> queue, long userId,
                                 int totalCountLeft,
                                 String token) {
        this.queue = queue;
        this.userId = userId;
        this.totalCountLeft = totalCountLeft;
        this.token = token;
    }

    @Override protected UsersFollowingLookupResponse executeImpl() {
        try {
            UsersFollowingLookupResponse uflr = queue.getNextApi().users().usersIdFollowing(
                String.valueOf(getUserId()),
                getCountForThisRun(),
                token
            );

            if (getCountLeft() > 0
                && (uflr.getMeta() != null && uflr.getMeta().getResultCount() != null
                && uflr.getMeta().getResultCount() == getCountForThisRun())) {
                queue.offer(new FollowsLookupRequest(
                    queue,
                    getUserId(),
                    getCountLeft(),
                    uflr.getMeta() != null ? uflr.getMeta().getNextToken() : null
                ));
            }

            return uflr;
        } catch (ApiException e) {
            if (e.getResponseHeaders() != null && e.getResponseHeaders().containsKey("x-rate-limit-remaining")) {
                CrawlerHandler.getInstance().handleRateLimit(
                    this,
                    Instant.ofEpochSecond(Long.parseLong(e.getResponseHeaders().get("x-rate-limit-reset").get(0)))
                );
            } else {
                log.severe("Could not get rate limit information from response headers. " + this.getClass().getSimpleName());
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override protected void runAfterExecutionImpl(UsersFollowingLookupResponse result) {
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
                        dm.insertFollowings(
                            getUserId(),
                            result.getData().stream()
                                .map(User::getId)
                                .map(Long::parseLong)
                                .collect(Collectors.toList()));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
        }
    }

    public long getUserId() {
        return userId;
    }

    private int getCountForThisRun() {
        return Math.max(10, Math.min(totalCountLeft, 1000));
    }

    private int getCountLeft() {
        return Math.max(0, totalCountLeft - Math.min(totalCountLeft, 1000));
    }
}
