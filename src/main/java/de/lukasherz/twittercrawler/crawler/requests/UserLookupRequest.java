package de.lukasherz.twittercrawler.crawler.requests;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.MultiUserLookupResponse;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import de.lukasherz.twittercrawler.crawler.Request;
import de.lukasherz.twittercrawler.crawler.RequestPriorityQueue;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import lombok.extern.java.Log;
import org.jetbrains.annotations.NotNull;

@Log
public class UserLookupRequest extends Request<MultiUserLookupResponse> {

    private final RequestPriorityQueue<MultiUserLookupResponse> queue;
    private final List<Long> userIds;
    private final Runnable runAfterExecution;

    public UserLookupRequest(RequestPriorityQueue<MultiUserLookupResponse> queue, @NotNull List<Long> userIds) {
        this.queue = queue;
        this.userIds = userIds;
        this.runAfterExecution = null;
    }

    public UserLookupRequest(RequestPriorityQueue<MultiUserLookupResponse> queue, List<Long> userIds,
                             Runnable runAfterExecution) {
        this.queue = queue;
        this.userIds = userIds;
        this.runAfterExecution = runAfterExecution;
    }

    @Override protected MultiUserLookupResponse executeImpl() {
        try {
            MultiUserLookupResponse mulr = queue.getNextApi().users().findUsersById(
                getUserIdsForThisRun().stream().map(String::valueOf).toList(),
                Set.of(
                    "author_id",
                    "entities.mentions.username",
                    "in_reply_to_user_id",
                    "referenced_tweets.id",
                    "referenced_tweets.id.author_id",
                    "geo.place_id"
                ),
                null,
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
                )
            );

            if (getUserIdsLeft().size() > 0
                && (mulr.getData() != null
                && mulr.getData().size() == getUserIdsForThisRun().size())) {
                queue.offer(new UserLookupRequest(
                    queue,
                    getUserIdsLeft(),
                    runAfterExecution
                ));
            }

            return mulr;
        } catch (ApiException e) {
            if (e.getResponseHeaders() != null && e.getResponseHeaders().containsKey("x-rate-limit-remaining")) {
                CrawlerHandler.getInstance().handleRateLimit(
                    this,
                    Instant.ofEpochSecond(Long.parseLong(e.getResponseHeaders().get("x-rate-limit-reset").get(0)))
                );
            } else {
                log.severe(
                    "Could not get rate limit information from response headers. " + this.getClass().getSimpleName());
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override protected MultiUserLookupResponse executeAndProcessImpl() {
        MultiUserLookupResponse mulr = executeImpl();
        processUserLookupRequestResult(mulr);
        return mulr;
    }

    private void processUserLookupRequestResult(MultiUserLookupResponse mulr) {
        if (mulr == null) {
            return;
        }

        DatabaseManager dm = DatabaseManager.getInstance();

        if (mulr.getData() != null) {
            try {
                dm.insertUsers(mulr.getData().stream().map(UserDbEntry::parse).toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override protected void runAfterExecutionImpl(MultiUserLookupResponse result) {
        if (runAfterExecution != null && getUserIdsLeft().size() == 0) {
            runAfterExecution.run();
        }
    }

    public @NotNull List<Long> getUserIds() {
        return userIds;
    }

    public @NotNull List<Long> getUserIdsForThisRun() {
        return userIds.subList(0, Math.min(userIds.size(), 100));
    }

    public @NotNull List<Long> getUserIdsLeft() {
        if (userIds.size() <= 100) {
            return List.of();
        }

        return userIds.subList(100, userIds.size());
    }
}
