package de.lukasherz.twittercrawler.crawler.requests;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.MultiUserLookupResponse;
import com.twitter.clientlib.model.User;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import de.lukasherz.twittercrawler.crawler.Request;
import de.lukasherz.twittercrawler.crawler.RequestPriorityQueue;
import de.lukasherz.twittercrawler.data.database.DatabaseManager;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

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
            e.printStackTrace();
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
        CrawlerHandler ch = CrawlerHandler.getInstance();

        if (mulr.getData() != null) {
            try {
                dm.insertUsers(mulr.getData().stream().map(UserDbEntry::parse).toList());
            } catch (SQLException e) {
                e.printStackTrace();
            }

            mulr.getData().stream()
                .map(User::getId)
                .map(Long::parseLong)
                .forEach(ch::addFollowsLookupToQuery);
        }
    }

    @Override protected void runAfterExecutionImpl(MultiUserLookupResponse result) {
        if (runAfterExecution != null) {
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
