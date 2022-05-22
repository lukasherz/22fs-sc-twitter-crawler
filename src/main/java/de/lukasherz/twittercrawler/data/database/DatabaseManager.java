package de.lukasherz.twittercrawler.data.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.lukasherz.twittercrawler.data.entities.tweets.*;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotation;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomain;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntity;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.TweetContextAnnotation;
import de.lukasherz.twittercrawler.data.entities.users.User;
import de.lukasherz.twittercrawler.data.entities.users.UserFollowing;
import lombok.Cleanup;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Log
public class DatabaseManager {
    private static DatabaseManager instance;
    private HikariDataSource hikariDataSource;

    private DatabaseManager() {
        log.info("DatabaseManager starting...");

        hikariDataSource = new HikariDataSource(createHikariConfig());

        // init database
        try {
            initDatabase();
        } catch (SQLException e) {
            log.severe("could not init database");
            e.printStackTrace();
        }

        log.info("DatabaseManager started");
    }

    public static DatabaseManager getInstance() {
        if (instance == null) {
            instance = new DatabaseManager();
        }
        return instance;
    }

    private static HikariConfig createHikariConfig() {
        @Cleanup InputStream is = DatabaseManager.class.getClassLoader().getResourceAsStream("config.properties");

        Properties properties = new Properties();

        if (is != null) {
            try {
                properties.load(is);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            log.severe("Could not load config.properties");
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.getProperty("jdbc.url"));
        config.setUsername(properties.getProperty("jdbc.username"));
        config.setPassword(properties.getProperty("jdbc.password"));
        config.addDataSourceProperty("useSSL", properties.getProperty("jdbc.useSSL"));
        config.addDataSourceProperty("serverTimezone", properties.getProperty("jdbc.serverTimezone"));

        return config;
    }

    private Connection getNewConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    private void initDatabase() throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS users" +
                        "(" +
                        "    id                  BIGINT," +
                        "    creation_date       datetime      NOT NULL," +
                        "    username            VARCHAR(15)   NOT NULL," +
                        "    name                VARCHAR(50)   NOT NULL," +
                        "    verified            BOOL          NOT NULL," +
                        "    profile_picture_url VARCHAR(1023) NULL," +
                        "    location            VARCHAR(255)  NULL," +
                        "    url                 VARCHAR(255)  NULL," +
                        "    biography           VARCHAR(160)  NULL," +
                        "    PRIMARY KEY (id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS user_following" +
                        "(" +
                        "    id           BIGINT AUTO_INCREMENT," +
                        "    user_id      BIGINT NOT NULL," +
                        "    following_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (user_id) REFERENCES users (id)," +
                        "    FOREIGN KEY (following_id) REFERENCES users (id)," +
                        "    UNIQUE (user_id, following_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS context_annotation_domains" +
                        "(" +
                        "    id          BIGINT," +
                        "    name        VARCHAR(255) NOT NULL," +
                        "    description VARCHAR(255) NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    UNIQUE (name)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS context_annotation_entities" +
                        "(" +
                        "    id          BIGINT," +
                        "    name        VARCHAR(255) NOT NULL," +
                        "    description VARCHAR(255) NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    UNIQUE (name)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweets" +
                        "(" +
                        "    id                    BIGINT," +
                        "    author_id             BIGINT        NOT NULL," +
                        "    text                  VARCHAR(1023) NOT NULL," +
                        "    created_at            datetime      NOT NULL," +
                        "    metrics_retweet_count INT           NOT NULL," +
                        "    metrics_like_count    INT           NOT NULL," +
                        "    metrics_reply_count   INT           NOT NULL," +
                        "    metrics_quote_count   INT           NOT NULL," +
                        "    lang                  VARCHAR(15)," +
                        "    geo                   VARCHAR(255)," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (author_id) REFERENCES users (id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS context_annotations" +
                        "(" +
                        "    id                           BIGINT AUTO_INCREMENT," +
                        "    context_annotation_domain_id BIGINT NOT NULL," +
                        "    context_annotation_entity_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (context_annotation_domain_id) REFERENCES context_annotation_domains (id)," +
                        "    FOREIGN KEY (context_annotation_entity_id) REFERENCES context_annotation_entities (id)," +
                        "    UNIQUE (context_annotation_entity_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweet_context_annotations" +
                        "(" +
                        "    id                    BIGINT AUTO_INCREMENT," +
                        "    tweet_id              BIGINT NOT NULL," +
                        "    context_annotation_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
                        "    FOREIGN KEY (context_annotation_id) REFERENCES context_annotations (id)," +
                        "    UNIQUE (tweet_id, context_annotation_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweet_references" +
                        "(" +
                        "    id                  BIGINT AUTO_INCREMENT," +
                        "    tweet_id            BIGINT      NOT NULL," +
                        "    referenced_tweet_id BIGINT      NOT NULL," +
                        "    reference_type      VARCHAR(15) NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
                        "    FOREIGN KEY (referenced_tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (tweet_id, referenced_tweet_id, reference_type)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS user_tweets" +
                        "(" +
                        "    id       BIGINT AUTO_INCREMENT," +
                        "    user_id  BIGINT NOT NULL," +
                        "    tweet_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (user_id) REFERENCES users (id)," +
                        "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (user_id, tweet_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweet_likes" +
                        "(" +
                        "    id       BIGINT AUTO_INCREMENT," +
                        "    user_id  BIGINT NOT NULL," +
                        "    tweet_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (user_id) REFERENCES users (id)," +
                        "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (user_id, tweet_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweet_retweets" +
                        "(" +
                        "    id       BIGINT AUTO_INCREMENT," +
                        "    user_id  BIGINT NOT NULL," +
                        "    tweet_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (user_id) REFERENCES users (id)," +
                        "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (user_id, tweet_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweet_replies" +
                        "(" +
                        "    id               BIGINT AUTO_INCREMENT," +
                        "    reply_tweet_id   BIGINT NOT NULL," +
                        "    replied_tweet_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (reply_tweet_id) REFERENCES tweets (id)," +
                        "    FOREIGN KEY (replied_tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (reply_tweet_id, replied_tweet_id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS tweet_quotes" +
                        "(" +
                        "    id              BIGINT AUTO_INCREMENT," +
                        "    quote_tweet_id  BIGINT NOT NULL," +
                        "    quoted_tweet_id BIGINT NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (quote_tweet_id) REFERENCES tweets (id)," +
                        "    FOREIGN KEY (quoted_tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (quote_tweet_id, quoted_tweet_id)" +
                        ");")) {
            ps.execute();
        }
    }

    public void insertUser(User user) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO users (id, creation_date, username, name, verified, profile_picture_url, location, url, biography) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setLong(1, user.getId());
            ps.setTimestamp(2, Timestamp.from(user.getCreationDate()));
            ps.setString(3, user.getUsername());
            ps.setString(4, user.getName());
            ps.setBoolean(5, user.isVerified());
            ps.setString(6, user.getProfilePictureUrl());
            ps.setString(7, user.getLocation());
            ps.setString(8, user.getUrl());
            ps.setString(9, user.getBiography());
            ps.execute();
        }
    }

    public boolean existsUser(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM users WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<User> getUser(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM users WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new User(
                            rs.getLong("id"),
                            rs.getTimestamp("creation_date").toInstant(),
                            rs.getString("username"),
                            rs.getString("name"),
                            rs.getBoolean("verified"),
                            rs.getString("profile_picture_url"),
                            rs.getString("location"),
                            rs.getString("url"),
                            rs.getString("biography")
                    ));
                }
            }
        }
        return Optional.empty();
    }

    public List<User> getAllUsers() throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM users")) {
            try (ResultSet rs = ps.executeQuery()) {
                List<User> users = new ArrayList<>();
                while (rs.next()) {
                    users.add(new User(
                            rs.getLong("id"),
                            rs.getTimestamp("creation_date").toInstant(),
                            rs.getString("username"),
                            rs.getString("name"),
                            rs.getBoolean("verified"),
                            rs.getString("profile_picture_url"),
                            rs.getString("location"),
                            rs.getString("url"),
                            rs.getString("biography")
                    ));
                }
                return users;
            }
        }
    }

    public Optional<UserFollowing> addFollowing(long userId, long followingId, boolean returnObject) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO user_following (user_id, following_id) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, userId);
            ps.setLong(2, followingId);

            ps.execute();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    if (returnObject) {
                        return Optional.of(new UserFollowing(
                                rs.getLong("id"),
                                getUser(userId).get(),
                                getUser(followingId).get()
                        ));
                    }
                }
            }
        }

        return Optional.empty();
    }

    public boolean isFollowing(long userId, long followingId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM user_following WHERE user_id = ? AND following_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, followingId);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<UserFollowing> getFollowing(long userId, long followingId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM user_following WHERE user_id = ? AND following_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, followingId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new UserFollowing(
                            rs.getLong("id"),
                            getUser(userId).get(),
                            getUser(followingId).get()
                    ));
                }
            }
        }
        return Optional.empty();
    }

    public List<UserFollowing> getAllFollowings(long userId) throws SQLException {
        List<UserFollowing> followings = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM user_following WHERE user_id = ?")) {
            ps.setLong(1, userId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    followings.add(new UserFollowing(
                            rs.getLong("id"),
                            getUser(userId).get(),
                            getUser(rs.getLong("following_id")).get()
                    ));
                }
            }
        }

        return followings;
    }

    public void insertContextAnnotationDomain(ContextAnnotationDomain cad) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO context_annotation_domains (id, name, description) VALUES (?, ?, ?);")) {
            ps.setLong(1, cad.getId());
            ps.setString(2, cad.getName());
            ps.setString(3, cad.getDescription());
            ps.execute();
        }
    }

    public boolean existsContextAnnotationDomain(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM context_annotation_domains WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<ContextAnnotationDomain> getContextAnnotationDomain(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_domains WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ContextAnnotationDomain(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return Optional.empty();
    }

    public List<ContextAnnotationDomain> getAllContextAnnotationDomains() throws SQLException {
        List<ContextAnnotationDomain> cad = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_domains")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cad.add(new ContextAnnotationDomain(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return cad;
    }

    public void insertContextAnnotationEntity(ContextAnnotationEntity cae) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO context_annotation_entities (id, name, description) VALUES (?, ?, ?);")) {
            ps.setLong(1, cae.getId());
            ps.setString(2, cae.getName());
            ps.setString(3, cae.getDescription());
            ps.execute();
        }
    }

    public boolean existsContextAnnotationEntity(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM context_annotation_entities WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<ContextAnnotationEntity> getContextAnnotationEntity(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_entities WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ContextAnnotationEntity(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return Optional.empty();
    }

    public List<ContextAnnotationEntity> getAllContextAnnotationEntities() throws SQLException {
        List<ContextAnnotationEntity> cae = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_entities")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cae.add(new ContextAnnotationEntity(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return cae;
    }

    public void insertContextAnnotation(ContextAnnotation ca) throws SQLException {
        if (!existsContextAnnotationDomain(ca.getContextAnnotationDomain().getId())) {
            insertContextAnnotationDomain(ca.getContextAnnotationDomain());
        }

        if (!existsContextAnnotationEntity(ca.getContextAnnotationEntity().getId())) {
            insertContextAnnotationEntity(ca.getContextAnnotationEntity());
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO context_annotations (context_annotation_domain_id, context_annotation_entity_id) VALUES (?, ?);")) {
            ps.setLong(1, ca.getContextAnnotationDomain().getId());
            ps.setLong(2, ca.getContextAnnotationEntity().getId());
            ps.execute();
        }
    }

    public boolean existsContextAnnotationByIds(ContextAnnotation ca) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM context_annotations WHERE context_annotation_domain_id = ? AND context_annotation_entity_id = ?")) {
            ps.setLong(1, ca.getContextAnnotationDomain().getId());
            ps.setLong(2, ca.getContextAnnotationEntity().getId());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<ContextAnnotation> getContextAnnotation(long contextAnnotationEntityId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, context_annotation_domain_id, context_annotation_entity_id FROM context_annotations WHERE context_annotation_entity_id = ?")) {
            ps.setLong(1, contextAnnotationEntityId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    ContextAnnotationEntity cae = getContextAnnotationEntity(rs.getLong("context_annotation_entity_id")).get();
                    ContextAnnotationDomain cad = getContextAnnotationDomain(rs.getLong("context_annotation_domain_id")).get();
                    return Optional.of(new ContextAnnotation(rs.getLong("id"), cad, cae));
                }
            }
        }
        return Optional.empty();
    }

    public List<ContextAnnotation> getAllContextAnnotations() throws SQLException {
        List<ContextAnnotation> cas = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, context_annotation_domain_id, context_annotation_entity_id FROM context_annotations")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ContextAnnotationEntity cae = getContextAnnotationEntity(rs.getLong("context_annotation_entity_id")).get();
                    ContextAnnotationDomain cad = getContextAnnotationDomain(rs.getLong("context_annotation_domain_id")).get();
                    cas.add(new ContextAnnotation(rs.getLong("id"), cad, cae));
                }
            }
        }
        return cas;
    }

    public void insertTweet(Tweet tweet) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweets (id, author_id, text, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo) VALUES (?, ?, ?, ?, ?, ?,?, ?,?,?);")) {
            ps.setLong(1, tweet.getId());
            ps.setLong(2, tweet.getAuthor().getId());
            ps.setString(3, tweet.getText());
            ps.setTimestamp(4, Timestamp.from(tweet.getCreatedAt()));
            ps.setInt(5, tweet.getMetricsRetweetCount());
            ps.setInt(6, tweet.getMetricsLikeCount());
            ps.setInt(7, tweet.getMetricsReplyCount());
            ps.setInt(8, tweet.getMetricsQuoteCount());
            ps.setString(9, tweet.getLang());
            ps.setString(10, tweet.getGeo());
        }
    }

    public boolean existsTweet(long tweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweets WHERE id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<Tweet> getTweet(long tweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, author_id, text, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo FROM tweets WHERE id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Tweet tweet = new Tweet(
                            rs.getLong("id"),
                            getUser(rs.getLong("author_id")).get(),
                            rs.getString("text"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getInt("metrics_retweet_count"),
                            rs.getInt("metrics_like_count"),
                            rs.getInt("metrics_reply_count"),
                            rs.getInt("metrics_quote_count"),
                            rs.getString("lang"),
                            rs.getString("geo"));
                    return Optional.of(tweet);
                }
            }
        }
        return Optional.empty();
    }

    public List<Tweet> getAllTweetsByUserId(long userId) throws SQLException {
        List<Tweet> tweets = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, author_id, text, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo FROM tweets WHERE author_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Tweet tweet = new Tweet(
                            rs.getLong("id"),
                            getUser(rs.getLong("author_id")).get(),
                            rs.getString("text"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getInt("metrics_retweet_count"),
                            rs.getInt("metrics_like_count"),
                            rs.getInt("metrics_reply_count"),
                            rs.getInt("metrics_quote_count"),
                            rs.getString("lang"),
                            rs.getString("geo"));
                    tweets.add(tweet);
                }
            }
        }
        return tweets;
    }

    public List<Tweet> getAllTweetsByUser(User user) throws SQLException {
        return getAllTweetsByUserId(user.getId());
    }

    public List<Tweet> getAllTweets() throws SQLException {
        List<Tweet> tweets = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, author_id, text, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo FROM tweets")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Tweet tweet = new Tweet(
                            rs.getLong("id"),
                            getUser(rs.getLong("author_id")).get(),
                            rs.getString("text"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getInt("metrics_retweet_count"),
                            rs.getInt("metrics_like_count"),
                            rs.getInt("metrics_reply_count"),
                            rs.getInt("metrics_quote_count"),
                            rs.getString("lang"),
                            rs.getString("geo"));
                    tweets.add(tweet);
                }
            }
        }
        return tweets;
    }

    public void insertTweetContextAnnotation(TweetContextAnnotation tweetContextAnnotation) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_context_annotations (tweet_id, context_annotation_id) VALUES (?, ?)")) {
            ps.setLong(1, tweetContextAnnotation.getTweet().getId());
            ps.setLong(2, tweetContextAnnotation.getContextAnnotation().getId());
            ps.executeUpdate();
        }
    }

    public boolean existsTweetContextAnnotation(TweetContextAnnotation tweetContextAnnotation) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT COUNT(*) FROM tweet_context_annotations WHERE tweet_id = ? AND context_annotation_id = ?")) {
            ps.setLong(1, tweetContextAnnotation.getTweet().getId());
            ps.setLong(2, tweetContextAnnotation.getContextAnnotation().getId());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    public Optional<TweetContextAnnotation> getTweetContextAnnotation(Tweet tweet, ContextAnnotation contextAnnotation) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, context_annotation_id FROM tweet_context_annotations WHERE tweet_id = ? AND context_annotation_id = ?")) {
            ps.setLong(1, tweet.getId());
            ps.setLong(2, contextAnnotation.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetContextAnnotation(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getContextAnnotation(rs.getLong("context_annotation_id")).get()));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetContextAnnotation> getAllContextAnnotationsOfTweet(long tweetId) throws SQLException {
        List<TweetContextAnnotation> tweetContextAnnotations = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, context_annotation_id FROM tweet_context_annotations WHERE tweet_id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetContextAnnotations.add(new TweetContextAnnotation(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getContextAnnotation(rs.getLong("context_annotation_id")).get()));
                }
            }
        }
        return tweetContextAnnotations;
    }

    public List<TweetContextAnnotation> getAllContextAnnotationsOfTweet(Tweet tweet) throws SQLException {
        return getAllContextAnnotationsOfTweet(tweet.getId());
    }

    public List<TweetContextAnnotation> getAllTweetContextAnnotations() throws SQLException {
        List<TweetContextAnnotation> tweetContextAnnotations = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, context_annotation_id FROM tweet_context_annotations")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetContextAnnotations.add(new TweetContextAnnotation(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getContextAnnotation(rs.getLong("context_annotation_id")).get()));
                }
            }
        }
        return tweetContextAnnotations;
    }

    public void insertTweetReference(TweetReference tweetReference) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_references (tweet_id, referenced_tweet_id, reference_type) VALUES (?, ?, ?)")) {
            ps.setLong(1, tweetReference.getTweet().getId());
            ps.setLong(2, tweetReference.getReferencedTweet().getId());
            ps.setString(3, tweetReference.getReferenceType());
            ps.executeUpdate();
        }
    }

    public boolean existsTweetReference(long tweetId, long referencedTweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_references WHERE tweet_id = ? AND referenced_tweet_id = ?")) {
            ps.setLong(1, tweetId);
            ps.setLong(2, referencedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public boolean existsTweetReference(Tweet tweet, Tweet referencedTweet) throws SQLException {
        return existsTweetReference(tweet.getId(), referencedTweet.getId());
    }

    public Optional<TweetReference> getTweetReference(Tweet tweet, Tweet referencedTweet) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references WHERE tweet_id = ? AND referenced_tweet_id = ?")) {
            ps.setLong(1, tweet.getId());
            ps.setLong(2, referencedTweet.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetReference(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getTweet(rs.getLong("referenced_tweet_id")).get(),
                            rs.getString("reference_type")));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetReference> getAllTweetReferencesByTweet(long tweetId) throws SQLException {
        List<TweetReference> tweetReferences = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references WHERE tweet_id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReferences.add(new TweetReference(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getTweet(rs.getLong("referenced_tweet_id")).get(),
                            rs.getString("reference_type")));
                }
            }
        }
        return tweetReferences;
    }

    public List<TweetReference> getAllTweetReferencesByTweet(Tweet tweet) throws SQLException {
        return getAllTweetReferencesByTweet(tweet.getId());
    }

    public List<TweetReference> getAllTweetReferencesByReferencedTweet(long referencedTweetId) throws SQLException {
        List<TweetReference> tweetReferences = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references WHERE referenced_tweet_id = ?")) {
            ps.setLong(1, referencedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReferences.add(new TweetReference(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getTweet(rs.getLong("referenced_tweet_id")).get(),
                            rs.getString("reference_type")));
                }
            }
        }
        return tweetReferences;
    }

    public List<TweetReference> getAllTweetReferencesByReferencedTweet(Tweet tweet) throws SQLException {
        return getAllTweetReferencesByReferencedTweet(tweet.getId());
    }

    public List<TweetReference> getAllTweetReferences() throws SQLException {
        List<TweetReference> tweetReferences = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReferences.add(new TweetReference(
                            rs.getLong("id"),
                            getTweet(rs.getLong("tweet_id")).get(),
                            getTweet(rs.getLong("referenced_tweet_id")).get(),
                            rs.getString("reference_type")));
                }
            }
        }
        return tweetReferences;
    }

    public void insertTweetLike(TweetLike tweetLike) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_likes (tweet_id, user_id) VALUES (?, ?)")) {
            ps.setLong(1, tweetLike.getTweet().getId());
            ps.setLong(2, tweetLike.getUser().getId());
            ps.executeUpdate();
        }
    }

    public boolean existsTweetLike(long likedTweetId, long userId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_likes WHERE tweet_id = ? AND user_id = ?")) {
            ps.setLong(1, likedTweetId);
            ps.setLong(2, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public boolean existsTweetLike(Tweet tweet, User user) throws SQLException {
        return existsTweetLike(tweet.getId(), user.getId());
    }

    public Optional<TweetLike> getTweetLike(Tweet tweet, User user) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_likes WHERE tweet_id = ? AND user_id = ?")) {
            ps.setLong(1, tweet.getId());
            ps.setLong(2, user.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetLike(
                            rs.getLong("id"),
                            getUser(user.getId()).get(),
                            getTweet(tweet.getId()).get()));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetLike> getAllTweetLikesByTweet(long likedTweetId) throws SQLException {
        List<TweetLike> tweetLikes = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id FROM tweet_likes WHERE tweet_id = ?")) {
            ps.setLong(1, likedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetLikes.add(new TweetLike(
                            rs.getLong("id"),
                            getUser(rs.getLong("user_id")).get(),
                            getTweet(likedTweetId).get()));
                }
            }
        }
        return tweetLikes;
    }

    public List<TweetLike> getAllTweetLikesByTweet(Tweet tweet) throws SQLException {
        return getAllTweetLikesByTweet(tweet.getId());
    }

    public List<TweetLike> getAllTweetLikesByUser(long userId) throws SQLException {
        List<TweetLike> tweetLikes = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id FROM tweet_likes WHERE user_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetLikes.add(new TweetLike(
                            rs.getLong("id"),
                            getUser(userId).get(),
                            getTweet(rs.getLong("tweet_id")).get()));
                }
            }
        }
        return tweetLikes;
    }

    public List<TweetLike> getAllTweetLikesByUser(User user) throws SQLException {
        return getAllTweetLikesByUser(user.getId());
    }

    public List<TweetLike> getAllTweetLikes() throws SQLException {
        List<TweetLike> tweetLikes = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id, tweet_id FROM tweet_likes")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetLikes.add(new TweetLike(
                            rs.getLong("id"),
                            getUser(rs.getLong("user_id")).get(),
                            getTweet(rs.getLong("tweet_id")).get()));
                }
            }
        }
        return tweetLikes;
    }

    public void insertTweetRetweet(TweetRetweet tweetRetweet) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_retweets (tweet_id, user_id) VALUES (?, ?)")) {
            ps.setLong(1, tweetRetweet.getTweet().getId());
            ps.setLong(2, tweetRetweet.getUser().getId());
            ps.executeUpdate();
        }
    }

    public boolean existsTweetRetweet(long retweetedTweetId, long userId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_retweets WHERE tweet_id = ? AND user_id = ?")) {
            ps.setLong(1, retweetedTweetId);
            ps.setLong(2, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public boolean existsTweetRetweet(Tweet tweet, User user) throws SQLException {
        return existsTweetRetweet(tweet.getId(), user.getId());
    }

    public Optional<TweetRetweet> getTweetRetweet(Tweet tweet, User user) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_retweets WHERE tweet_id = ? AND user_id = ?")) {
            ps.setLong(1, tweet.getId());
            ps.setLong(2, user.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetRetweet(
                            rs.getLong("id"),
                            getUser(user.getId()).get(),
                            getTweet(tweet.getId()).get()));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetRetweet> getAllTweetRetweetsByTweet(long retweetedTweetId) throws SQLException {
        List<TweetRetweet> tweetRetweets = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id FROM tweet_retweets WHERE tweet_id = ?")) {
            ps.setLong(1, retweetedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetRetweets.add(new TweetRetweet(
                            rs.getLong("id"),
                            getUser(rs.getLong("user_id")).get(),
                            getTweet(retweetedTweetId).get()));
                }
            }
        }
        return tweetRetweets;
    }

    public List<TweetRetweet> getAllTweetRetweetsByTweet(Tweet tweet) throws SQLException {
        return getAllTweetRetweetsByTweet(tweet.getId());
    }

    public List<TweetRetweet> getAllTweetRetweetsByUser(long userId) throws SQLException {
        List<TweetRetweet> tweetRetweets = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id FROM tweet_retweets WHERE user_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetRetweets.add(new TweetRetweet(
                            rs.getLong("id"),
                            getUser(userId).get(),
                            getTweet(rs.getLong("tweet_id")).get()));
                }
            }
        }
        return tweetRetweets;
    }

    public List<TweetRetweet> getAllTweetRetweetsByUser(User user) throws SQLException {
        return getAllTweetRetweetsByUser(user.getId());
    }

    public List<TweetRetweet> getAllTweetRetweets() throws SQLException {
        List<TweetRetweet> tweetRetweets = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id, tweet_id FROM tweet_retweets")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetRetweets.add(new TweetRetweet(
                            rs.getLong("id"),
                            getUser(rs.getLong("user_id")).get(),
                            getTweet(rs.getLong("tweet_id")).get()));
                }
            }
        }
        return tweetRetweets;
    }

    public void insertTweetReply(TweetReply tweetReply) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_replies (reply_tweet_id, replied_tweet_id) VALUES (?, ?)")) {
            ps.setLong(1, tweetReply.getReplyTweet().getId());
            ps.setLong(2, tweetReply.getRepliedTweet().getId());
            ps.executeUpdate();
        }
    }

    public boolean existsTweetReply(long repliedTweetId, long replyTweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_replies WHERE reply_tweet_id = ? AND replied_tweet_id = ?")) {
            ps.setLong(1, replyTweetId);
            ps.setLong(2, repliedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public boolean existsTweetReply(Tweet tweet, Tweet replyTweet) throws SQLException {
        return existsTweetReply(tweet.getId(), replyTweet.getId());
    }

    public Optional<TweetReply> getTweetReply(Tweet tweet, Tweet replyTweet) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_replies WHERE reply_tweet_id = ? AND replied_tweet_id = ?")) {
            ps.setLong(1, replyTweet.getId());
            ps.setLong(2, tweet.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetReply(
                            rs.getLong("id"),
                            getTweet(replyTweet.getId()).get(),
                            getTweet(tweet.getId()).get()));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetReply> getTweetRepliesByUser(long userId) throws SQLException {
        List<TweetReply> tweetReplies = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, reply_tweet_id, replied_tweet_id FROM tweet_replies INNER JOIN tweets ON reply_tweet_id = tweets.id WHERE tweets.author_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReplies.add(new TweetReply(
                            rs.getLong("id"),
                            getTweet(rs.getLong("reply_tweet_id")).get(),
                            getTweet(rs.getLong("replied_tweet_id")).get()));
                }
            }
        }
        return tweetReplies;
    }

    public List<TweetReply> getTweetRepliesByUser(User user) throws SQLException {
        return getTweetRepliesByUser(user.getId());
    }

    public List<TweetReply> getTweetRepliesByUserAndRepliedTweet(long userId, long repliedTweetId) throws SQLException {
        List<TweetReply> tweetReplies = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, reply_tweet_id, replied_tweet_id FROM tweet_replies INNER JOIN tweets ON reply_tweet_id = tweets.id WHERE tweets.author_id = ? AND replied_tweet_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, repliedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReplies.add(new TweetReply(
                            rs.getLong("id"),
                            getTweet(rs.getLong("reply_tweet_id")).get(),
                            getTweet(rs.getLong("replied_tweet_id")).get()));
                }
            }
        }
        return tweetReplies;
    }

    public List<TweetReply> getTweetRepliesByRepliedTweet(long repliedTweetId) throws SQLException {
        List<TweetReply> tweetReplies = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, reply_tweet_id, replied_tweet_id FROM tweet_replies WHERE replied_tweet_id = ?")) {
            ps.setLong(1, repliedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReplies.add(new TweetReply(
                            rs.getLong("id"),
                            getTweet(rs.getLong("reply_tweet_id")).get(),
                            getTweet(rs.getLong("replied_tweet_id")).get()));
                }
            }
        }
        return tweetReplies;
    }

    public List<TweetReply> getTweetRepliesByRepliedTweet(Tweet tweet) throws SQLException {
        return getTweetRepliesByRepliedTweet(tweet.getId());
    }

    public void insertTweetQuote(TweetQuote tweetQuote) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_quotes (quote_tweet_id, quoted_tweet_id) VALUES (?, ?)")) {
            ps.setLong(1, tweetQuote.getQuoteTweet().getId());
            ps.setLong(2, tweetQuote.getQuotedTweet().getId());
            ps.executeUpdate();
        }
    }

    public boolean existsTweetQuote(long tweetId, long quoteTweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_quotes WHERE quote_tweet_id = ? AND quoted_tweet_id = ?")) {
            ps.setLong(1, quoteTweetId);
            ps.setLong(2, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public boolean existsTweetQuote(Tweet tweet, Tweet quoteTweet) throws SQLException {
        return existsTweetQuote(tweet.getId(), quoteTweet.getId());
    }

    public Optional<TweetQuote> getTweetQuote(Tweet tweet, Tweet quoteTweet) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_quotes WHERE quote_tweet_id = ? AND quoted_tweet_id = ?")) {
            ps.setLong(1, quoteTweet.getId());
            ps.setLong(2, tweet.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetQuote(
                            rs.getLong("id"),
                            getTweet(quoteTweet.getId()).get(),
                            getTweet(tweet.getId()).get()));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetQuote> getTweetQuotesByUser(long userId) throws SQLException {
        List<TweetQuote> tweetQuotes = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, quote_tweet_id, quoted_tweet_id FROM tweet_quotes INNER JOIN tweets ON quote_tweet_id = tweets.id WHERE tweets.author_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetQuotes.add(new TweetQuote(
                            rs.getLong("id"),
                            getTweet(rs.getLong("quote_tweet_id")).get(),
                            getTweet(rs.getLong("quoted_tweet_id")).get()));
                }
            }
        }
        return tweetQuotes;
    }

    public List<TweetQuote> getTweetQuotesByUser(User user) throws SQLException {
        return getTweetQuotesByUser(user.getId());
    }

    public List<TweetQuote> getTweetQuotesByUserAndQuotedTweet(long userId, long quotedTweetId) throws SQLException {
        List<TweetQuote> tweetQuotes = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, quote_tweet_id, quoted_tweet_id FROM tweet_quotes INNER JOIN tweets ON quote_tweet_id = tweets.id WHERE tweets.author_id = ? AND quoted_tweet_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, quotedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetQuotes.add(new TweetQuote(
                            rs.getLong("id"),
                            getTweet(rs.getLong("quote_tweet_id")).get(),
                            getTweet(rs.getLong("quoted_tweet_id")).get()));
                }
            }
        }
        return tweetQuotes;
    }

    public List<TweetQuote> getTweetQuotesByQuotedTweet(long quotedTweetId) throws SQLException {
        List<TweetQuote> tweetQuotes = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, quote_tweet_id, quoted_tweet_id FROM tweet_quotes WHERE quoted_tweet_id = ?")) {
            ps.setLong(1, quotedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetQuotes.add(new TweetQuote(
                            rs.getLong("id"),
                            getTweet(rs.getLong("quote_tweet_id")).get(),
                            getTweet(rs.getLong("quoted_tweet_id")).get()));
                }
            }
        }
        return tweetQuotes;
    }

    public List<TweetQuote> getTweetQuotesByQuotedTweet(Tweet tweet) throws SQLException {
        return getTweetQuotesByQuotedTweet(tweet.getId());
    }
}
