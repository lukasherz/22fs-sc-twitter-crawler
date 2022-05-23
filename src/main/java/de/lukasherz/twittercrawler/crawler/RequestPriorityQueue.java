package de.lukasherz.twittercrawler.crawler;

import com.twitter.clientlib.api.TwitterApi;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class RequestPriorityQueue<T> {
    private final PriorityBlockingQueue<Request<T>> queue;
    private final SortedSet<Map.Entry<TwitterApi, Instant>> nextApi;

    public RequestPriorityQueue(Set<TwitterApi> apis) {
        this(apis, 100);
    }

    public RequestPriorityQueue(Set<TwitterApi> apis, int initialCapacity) {
        this.queue = new PriorityBlockingQueue<>(initialCapacity, Comparator.comparing(Request::getPriority));

        this.nextApi = Collections.synchronizedSortedSet(new TreeSet<>(Map.Entry.comparingByValue()));
        this.nextApi.addAll(apis.stream().map(api -> Map.entry(api, Instant.now())).toList());
    }

    public void offer(Request<T> request) {
        queue.offer(request);
    }

    public Request<T> poll() {
        return queue.poll();
    }

    public Request<T> peek() {
        return queue.peek();
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public Request<T> takeOrWait() throws InterruptedException {
        return queue.take();
    }

    public void clear() {
        queue.clear();
    }

    public @NotNull Map.Entry<TwitterApi, Instant> getNextApiEntry() {
        return nextApi.first();
    }

    public @NotNull TwitterApi getNextApi() {
        return nextApi.first().getKey();
    }

    public void setTimeForApi(@NotNull TwitterApi api, @NotNull Instant time) {
        nextApi.removeIf(e -> e.getKey().equals(api));
        nextApi.add(Map.entry(api, time));
    }

    public void resetTimeForApis() {
        nextApi.forEach(e -> e.setValue(Instant.now()));
    }

    public boolean canRequest() {
        return !nextApi.isEmpty() && nextApi.first().getValue().isBefore(Instant.now());
    }
}
