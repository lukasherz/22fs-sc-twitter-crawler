package de.lukasherz.twittercrawler.crawler;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public abstract class Request<T> {

    private final Priority priority = Priority.NORMAL;

    protected abstract T executeImpl();

    public T execute() {
        T result = executeImpl();
        runAfterExecutionImpl(result);
        return result;
    }

    protected T executeAndProcessImpl() {
        return execute();
    }

    public T executeAndProcess() {
        T result = executeAndProcessImpl();
        runAfterExecutionImpl(result);
        return result;
    }

    protected void runAfterExecutionImpl(T result) {

    }

    enum Priority {
        HIGHEST(2),
        HIGH(1),
        NORMAL(0),
        LOW(-1),
        LOWEST(-2);

        private final int value;

        Priority(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
