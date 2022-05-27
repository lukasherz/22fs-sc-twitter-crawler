package de.lukasherz.twittercrawler.crawler;

import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;

public class QueuedTimer<T> {

    private final RequestPriorityQueue<T> queue;
    private final String name;
    private Timer timer;
    private boolean running;

    public QueuedTimer(RequestPriorityQueue<T> queue, String name) {
        this.queue = queue;
        this.name = name;
    }

    private TimerTask getNewTimer() {
        return new TimerTask() {
            @Override public void run() {
                running = true;
                while (!queue.isEmpty() && queue.getNextApiEntry().getValue().isBefore(Instant.now())) {
                    queue.poll().executeAndProcess();
                }

                running = false;

                if (queue.getNextApiEntry().getValue().isAfter(Instant.now())) {
                    schedule(queue.getNextApiEntry().getValue());
                } else {
                    schedule(Instant.now().plus(5, ChronoUnit.SECONDS));
                }
            }
        };
    }

    public void start() {
        if (running) return;

        schedule(Instant.now().plus(1, ChronoUnit.SECONDS));
    }

    public void stop() {
        if (!running) return;

        timer.cancel();
    }

    public boolean isRunning() {
        return running;
    }

    public void schedule(Instant time) {
        if (running) {
            stop();
        }

        this.timer = new Timer(name);
        this.timer.schedule(getNewTimer(), Date.from(time));
    }
}
