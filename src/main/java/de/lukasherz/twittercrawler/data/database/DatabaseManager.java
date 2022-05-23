package de.lukasherz.twittercrawler.data.database;

import com.twitter.clientlib.model.Tweet;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.lukasherz.twittercrawler.data.entities.tweets.*;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomainDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntityDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.TweetContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserFollowingDbEntry;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;

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
        InputStream is = DatabaseManager.class.getClassLoader().getResourceAsStream("config.properties");

        Properties properties = new Properties();

        if (is != null) {
            try {
                properties.load(is);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

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
                        "    biography           VARCHAR(255)  NULL," +
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
                        "    description VARCHAR(1023)," +
                        "    PRIMARY KEY (id)" +
                        ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS context_annotation_entities" +
                        "(" +
                        "    id          BIGINT," +
                        "    name        VARCHAR(255) NOT NULL," +
                        "    description VARCHAR(1023)," +
                        "    PRIMARY KEY (id)" +
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
                        "    search_query          VARCHAR(255)  DEFAULT NULL," +
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
                        "    UNIQUE KEY (context_annotation_domain_id, context_annotation_entity_id)" +
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
                "CREATE TABLE IF NOT EXISTS tweet_references_extern(" +
                        "    id                  BIGINT AUTO_INCREMENT," +
                        "    tweet_id            BIGINT      NOT NULL," +
                        "    referenced_tweet_id BIGINT      NOT NULL," +
                        "    reference_type      VARCHAR(15) NOT NULL," +
                        "    PRIMARY KEY (id)," +
                        "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
                        "    UNIQUE (tweet_id, referenced_tweet_id, reference_type)" +
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

    public void insertUser(UserDbEntry userDbEntry) throws SQLException {
        insertUsers(Collections.singletonList(userDbEntry));
    }

    public void insertUsers(List<UserDbEntry> userDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO users (id, creation_date, username, name, verified, profile_picture_url, location, url, " +
                        "biography) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (UserDbEntry userDbEntry : userDbEntries) {
                ps.setLong(1, userDbEntry.getId());
                ps.setTimestamp(2, Timestamp.from(userDbEntry.getCreationDate()));
                ps.setString(3, userDbEntry.getUsername());
                ps.setString(4, userDbEntry.getName());
                ps.setBoolean(5, userDbEntry.isVerified());
                ps.setString(6, userDbEntry.getProfilePictureUrl());
                ps.setString(7, userDbEntry.getLocation());
                ps.setString(8, userDbEntry.getUrl());
                ps.setString(9, userDbEntry.getBiography());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public Optional<UserDbEntry> getUser(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM users WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new UserDbEntry(
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

    public List<UserDbEntry> getAllUsers() throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM users")) {
            try (ResultSet rs = ps.executeQuery()) {
                List<UserDbEntry> userDbEntries = new ArrayList<>();
                while (rs.next()) {
                    userDbEntries.add(new UserDbEntry(
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
                return userDbEntries;
            }
        }
    }

    public void insertFollowing(long userId, long followingId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO user_following (user_id, following_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            ps.setLong(1, userId);
            ps.setLong(2, followingId);

            ps.execute();
        }
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

    public Optional<UserFollowingDbEntry> getFollowing(long userId, long followingId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM user_following WHERE user_id = ? AND following_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, followingId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new UserFollowingDbEntry(
                            rs.getLong("id"),
                            rs.getLong("user_id"),
                            rs.getLong("following_id")
                    ));
                }
            }
        }
        return Optional.empty();
    }

    public List<UserFollowingDbEntry> getAllFollowings(long userId) throws SQLException {
        List<UserFollowingDbEntry> followings = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM user_following WHERE user_id = ?")) {
            ps.setLong(1, userId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    followings.add(new UserFollowingDbEntry(
                            rs.getLong("id"),
                            rs.getLong("user_id"),
                            rs.getLong("following_id")
                    ));
                }
            }
        }

        return followings;
    }

    public void insertContextAnnotationDomain(ContextAnnotationDomainDbEntry cad) throws SQLException {
        insertContextAnnotationDomains(Collections.singletonList(cad));
    }

    public void insertContextAnnotationDomains(List<ContextAnnotationDomainDbEntry> cad) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO context_annotation_domains (id, name, description) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = id;")) {
            for (ContextAnnotationDomainDbEntry ca : cad) {
                ps.setLong(1, ca.getId());
                ps.setString(2, ca.getName());
                ps.setString(3, ca.getDescription());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public Optional<ContextAnnotationDomainDbEntry> getContextAnnotationDomain(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_domains WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ContextAnnotationDomainDbEntry(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return Optional.empty();
    }

    public List<ContextAnnotationDomainDbEntry> getAllContextAnnotationDomains() throws SQLException {
        List<ContextAnnotationDomainDbEntry> cad = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_domains")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cad.add(new ContextAnnotationDomainDbEntry(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return cad;
    }

    public void insertContextAnnotationEntity(ContextAnnotationEntityDbEntry cae) throws SQLException {
        insertContextAnnotationEntities(Collections.singletonList(cae));
    }

    public void insertContextAnnotationEntities(List<ContextAnnotationEntityDbEntry> cae) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO context_annotation_entities (id, name, description) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = id;")) {
            for (ContextAnnotationEntityDbEntry ca : cae) {
                ps.setLong(1, ca.getId());
                ps.setString(2, ca.getName());
                ps.setString(3, ca.getDescription());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public Optional<ContextAnnotationEntityDbEntry> getContextAnnotationEntity(long id) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_entities WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ContextAnnotationEntityDbEntry(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return Optional.empty();
    }

    public List<ContextAnnotationEntityDbEntry> getAllContextAnnotationEntities() throws SQLException {
        List<ContextAnnotationEntityDbEntry> cae = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, name, description FROM context_annotation_entities")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cae.add(new ContextAnnotationEntityDbEntry(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("description")));
                }
            }
        }
        return cae;
    }

    public void insertContextAnnotation(ContextAnnotationDbEntry ca) throws SQLException {
        insertContextAnnotations(Collections.singletonList(ca));
    }

    public void insertContextAnnotations(List<ContextAnnotationDbEntry> cas) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO context_annotations (context_annotation_domain_id, context_annotation_entity_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id;")) {
            for (ContextAnnotationDbEntry ca : cas) {
                ps.setLong(1, ca.getContextAnnotationDomainId());
                ps.setLong(2, ca.getContextAnnotationEntityId());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public boolean existsContextAnnotationByIds(ContextAnnotationDbEntry ca) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM context_annotations WHERE context_annotation_domain_id = ? AND context_annotation_entity_id = ?")) {
            ps.setLong(1, ca.getContextAnnotationDomainId());
            ps.setLong(2, ca.getContextAnnotationEntityId());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Optional<ContextAnnotationDbEntry> getContextAnnotation(long contextAnnotationEntityId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, context_annotation_domain_id, context_annotation_entity_id FROM context_annotations WHERE context_annotation_entity_id = ?")) {
            ps.setLong(1, contextAnnotationEntityId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ContextAnnotationDbEntry(
                            rs.getLong("id"),
                            rs.getLong("context_annotation_domain_id"),
                            rs.getLong("context_annotation_entity_id"))
                    );
                }
            }
        }
        return Optional.empty();
    }

    public List<ContextAnnotationDbEntry> getAllContextAnnotations() throws SQLException {
        List<ContextAnnotationDbEntry> cas = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, context_annotation_domain_id, context_annotation_entity_id FROM context_annotations")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cas.add(new ContextAnnotationDbEntry(
                            rs.getLong("id"),
                            rs.getLong("context_annotation_domain_id"),
                            rs.getLong("context_annotation_entity_id"))
                    );
                }
            }
        }
        return cas;
    }

    public void insertTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        insertTweets(Collections.singletonList(tweetDbEntry));
    }

    public void insertTweets(List<TweetDbEntry> tweetDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweets (id, author_id, text, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo, search_query) VALUES (?, ?, ?, ?, ?, ?, ?,?, ?,?,?) ON DUPLICATE KEY UPDATE id = id;")) {
            for (TweetDbEntry tweetDbEntry : tweetDbEntries) {
                ps.setLong(1, tweetDbEntry.getId());
                ps.setLong(2, tweetDbEntry.getAuthorId());
                ps.setString(3, tweetDbEntry.getText());
                ps.setTimestamp(4, Timestamp.from(tweetDbEntry.getCreatedAt()));
                ps.setInt(5, tweetDbEntry.getMetricsRetweetCount());
                ps.setInt(6, tweetDbEntry.getMetricsLikeCount());
                ps.setInt(7, tweetDbEntry.getMetricsReplyCount());
                ps.setInt(8, tweetDbEntry.getMetricsQuoteCount());
                ps.setString(9, tweetDbEntry.getLang());
                ps.setString(10, tweetDbEntry.getGeo());
                ps.setString(11, tweetDbEntry.getSearchQuery());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public Optional<TweetDbEntry> getTweet(long tweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, author_id, text, search_query, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo FROM tweets WHERE id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    TweetDbEntry tweetDbEntry = new TweetDbEntry(
                            rs.getLong("id"),
                            rs.getLong("author_id"),
                            rs.getString("text"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getString("search_query"),
                            rs.getInt("metrics_retweet_count"),
                            rs.getInt("metrics_like_count"),
                            rs.getInt("metrics_reply_count"),
                            rs.getInt("metrics_quote_count"),
                            rs.getString("lang"),
                            rs.getString("geo"));
                    return Optional.of(tweetDbEntry);
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetDbEntry> getAllTweetsByUserId(long userId) throws SQLException {
        List<TweetDbEntry> tweetDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, author_id, text, search_query, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo FROM tweets WHERE author_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TweetDbEntry tweetDbEntry = new TweetDbEntry(
                            rs.getLong("id"),
                            rs.getLong("author_id"),
                            rs.getString("text"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getString("search_query"),
                            rs.getInt("metrics_retweet_count"),
                            rs.getInt("metrics_like_count"),
                            rs.getInt("metrics_reply_count"),
                            rs.getInt("metrics_quote_count"),
                            rs.getString("lang"),
                            rs.getString("geo"));
                    tweetDbEntries.add(tweetDbEntry);
                }
            }
        }
        return tweetDbEntries;
    }

    public List<TweetDbEntry> getAllTweetsByUser(UserDbEntry userDbEntry) throws SQLException {
        return getAllTweetsByUserId(userDbEntry.getId());
    }

    public List<TweetDbEntry> getAllTweets() throws SQLException {
        List<TweetDbEntry> tweetDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, author_id, text, search_query, created_at, metrics_retweet_count, metrics_like_count, metrics_reply_count, metrics_quote_count, lang, geo FROM tweets")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TweetDbEntry tweetDbEntry = new TweetDbEntry(
                            rs.getLong("id"),
                            rs.getLong("author_id"),
                            rs.getString("text"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getString("search_query"),
                            rs.getInt("metrics_retweet_count"),
                            rs.getInt("metrics_like_count"),
                            rs.getInt("metrics_reply_count"),
                            rs.getInt("metrics_quote_count"),
                            rs.getString("lang"),
                            rs.getString("geo"));
                    tweetDbEntries.add(tweetDbEntry);
                }
            }
        }
        return tweetDbEntries;
    }

    public void insertTweetContextAnnotation(TweetContextAnnotationDbEntry tweetContextAnnotationDbEntry) throws SQLException {
        insertTweetContextAnnotations(Collections.singletonList(tweetContextAnnotationDbEntry));
    }

    public void insertTweetContextAnnotations(List<TweetContextAnnotationDbEntry> tweetContextAnnotationDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_context_annotations (tweet_id, context_annotation_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (TweetContextAnnotationDbEntry tweetContextAnnotationDbEntry : tweetContextAnnotationDbEntries) {
                ps.setLong(1, tweetContextAnnotationDbEntry.getTweetId());
                ps.setLong(2, tweetContextAnnotationDbEntry.getContextAnnotationId());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public void insertTweetContextAnnotation(long tweetId, long contextAnnotationDomainId, long contextAnnotationEntityId) throws SQLException {
        insertTweetContextAnnotations(tweetId, Collections.singletonList(contextAnnotationDomainId), Collections.singletonList(contextAnnotationEntityId));
    }

    public void insertTweetContextAnnotations(long tweetId, List<Long> contextAnnotationDomainIds, List<Long> contextAnnotationEntityIds) throws SQLException {
        if (contextAnnotationDomainIds.size() != contextAnnotationEntityIds.size()) {
            throw new IllegalArgumentException("contextAnnotationDomainIds and contextAnnotationEntityIds must have the same size");
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_context_annotations (tweet_id, context_annotation_id) VALUES (?, (SELECT id FROM context_annotations WHERE context_annotation_domain_id = ? AND context_annotation_entity_id = ?)) ON DUPLICATE KEY UPDATE id = id")) {
            for (int i = 0; i < contextAnnotationDomainIds.size(); i++) {
                ps.setLong(1, tweetId);
                ps.setLong(2, contextAnnotationDomainIds.get(i));
                ps.setLong(3, contextAnnotationEntityIds.get(i));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public boolean existsTweetContextAnnotation(TweetContextAnnotationDbEntry tweetContextAnnotationDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT COUNT(*) FROM tweet_context_annotations WHERE tweet_id = ? AND context_annotation_id = ?")) {
            ps.setLong(1, tweetContextAnnotationDbEntry.getTweetId());
            ps.setLong(2, tweetContextAnnotationDbEntry.getContextAnnotationId());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    public Optional<TweetContextAnnotationDbEntry> getTweetContextAnnotation(TweetDbEntry tweetDbEntry, ContextAnnotationDbEntry contextAnnotationDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, context_annotation_id FROM tweet_context_annotations WHERE tweet_id = ? AND context_annotation_id = ?")) {
            ps.setLong(1, tweetDbEntry.getId());
            ps.setLong(2, contextAnnotationDbEntry.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetContextAnnotationDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("context_annotation_id"))
                    );
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetContextAnnotationDbEntry> getAllContextAnnotationsOfTweet(long tweetId) throws SQLException {
        List<TweetContextAnnotationDbEntry> tweetContextAnnotationDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, context_annotation_id FROM tweet_context_annotations WHERE tweet_id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetContextAnnotationDbEntries.add(new TweetContextAnnotationDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("context_annotation_id"))
                    );
                }
            }
        }
        return tweetContextAnnotationDbEntries;
    }

    public List<TweetContextAnnotationDbEntry> getAllContextAnnotationsOfTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        return getAllContextAnnotationsOfTweet(tweetDbEntry.getId());
    }

    public List<TweetContextAnnotationDbEntry> getAllTweetContextAnnotations() throws SQLException {
        List<TweetContextAnnotationDbEntry> tweetContextAnnotationDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, context_annotation_id FROM tweet_context_annotations")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetContextAnnotationDbEntries.add(new TweetContextAnnotationDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("context_annotation_id"))
                    );
                }
            }
        }
        return tweetContextAnnotationDbEntries;
    }

    public void insertTweetReference(TweetReferenceDbEntry tweetReferenceDbEntry) throws SQLException {
        insertTweetReferences(Collections.singletonList(tweetReferenceDbEntry));
    }

    public void insertTweetReferences(List<TweetReferenceDbEntry> tweetReferenceDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_references_extern(tweet_id, referenced_tweet_id, reference_type) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (TweetReferenceDbEntry tweetReferenceDbEntry : tweetReferenceDbEntries) {
                ps.setLong(1, tweetReferenceDbEntry.getTweetId());
                ps.setLong(2, tweetReferenceDbEntry.getReferencedTweetId());
                ps.setString(3, tweetReferenceDbEntry.getReferenceType());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public boolean existsTweetReference(long tweetId, long referencedTweetId) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_references_externWHERE tweet_id = ? AND referenced_tweet_id = ?")) {
            ps.setLong(1, tweetId);
            ps.setLong(2, referencedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public boolean existsTweetReference(TweetDbEntry tweetDbEntry, TweetDbEntry referencedTweetDbEntry) throws SQLException {
        return existsTweetReference(tweetDbEntry.getId(), referencedTweetDbEntry.getId());
    }

    public Optional<TweetReferenceDbEntry> getTweetReference(TweetDbEntry tweetDbEntry, TweetDbEntry referencedTweetDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_externWHERE tweet_id = ? AND referenced_tweet_id = ?")) {
            ps.setLong(1, tweetDbEntry.getId());
            ps.setLong(2, referencedTweetDbEntry.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetReferenceDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("referenced_tweet_id"),
                            rs.getString("reference_type")));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetReferenceDbEntry> getAllTweetReferencesByTweet(long tweetId) throws SQLException {
        List<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_externWHERE tweet_id = ?")) {
            ps.setLong(1, tweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReferenceDbEntries.add(new TweetReferenceDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("referenced_tweet_id"),
                            rs.getString("reference_type")));
                }
            }
        }
        return tweetReferenceDbEntries;
    }

    public List<TweetReferenceDbEntry> getAllTweetReferencesByTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        return getAllTweetReferencesByTweet(tweetDbEntry.getId());
    }

    public List<TweetReferenceDbEntry> getAllTweetReferencesByReferencedTweet(long referencedTweetId) throws SQLException {
        List<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_externWHERE referenced_tweet_id = ?")) {
            ps.setLong(1, referencedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReferenceDbEntries.add(new TweetReferenceDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("referenced_tweet_id"),
                            rs.getString("reference_type")));
                }
            }
        }
        return tweetReferenceDbEntries;
    }

    public List<TweetReferenceDbEntry> getAllTweetReferencesByReferencedTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        return getAllTweetReferencesByReferencedTweet(tweetDbEntry.getId());
    }

    public List<TweetReferenceDbEntry> getAllTweetReferences() throws SQLException {
        List<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReferenceDbEntries.add(new TweetReferenceDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("referenced_tweet_id"),
                            rs.getString("reference_type")));
                }
            }
        }
        return tweetReferenceDbEntries;
    }

    public void insertTweetLike(TweetLikeDbEntry tweetLikeDbEntry) throws SQLException {
        insertTweetLikes(Collections.singletonList(tweetLikeDbEntry));
    }

    public void insertTweetLikes(List<TweetLikeDbEntry> tweetLikeDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_likes (tweet_id, user_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (TweetLikeDbEntry tweetLikeDbEntry : tweetLikeDbEntries) {
                ps.setLong(1, tweetLikeDbEntry.getTweetId());
                ps.setLong(2, tweetLikeDbEntry.getUserId());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public boolean existsTweetLike(TweetDbEntry tweetDbEntry, UserDbEntry userDbEntry) throws SQLException {
        return existsTweetLike(tweetDbEntry.getId(), userDbEntry.getId());
    }

    public Optional<TweetLikeDbEntry> getTweetLike(TweetDbEntry tweetDbEntry, UserDbEntry userDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_likes WHERE tweet_id = ? AND user_id = ?")) {
            ps.setLong(1, tweetDbEntry.getId());
            ps.setLong(2, userDbEntry.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetLikeDbEntry(
                            rs.getLong("id"),
                            tweetDbEntry.getId(),
                            userDbEntry.getId())
                    );
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetLikeDbEntry> getAllTweetLikesByTweet(long likedTweetId) throws SQLException {
        List<TweetLikeDbEntry> tweetLikeDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id FROM tweet_likes WHERE tweet_id = ?")) {
            ps.setLong(1, likedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetLikeDbEntries.add(new TweetLikeDbEntry(
                            rs.getLong("id"),
                            likedTweetId,
                            rs.getLong("user_id"))
                    );
                }
            }
        }
        return tweetLikeDbEntries;
    }

    public List<TweetLikeDbEntry> getAllTweetLikesByTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        return getAllTweetLikesByTweet(tweetDbEntry.getId());
    }

    public List<TweetLikeDbEntry> getAllTweetLikesByUser(long userId) throws SQLException {
        List<TweetLikeDbEntry> tweetLikeDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id FROM tweet_likes WHERE user_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetLikeDbEntries.add(new TweetLikeDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            userId)
                    );
                }
            }
        }
        return tweetLikeDbEntries;
    }

    public List<TweetLikeDbEntry> getAllTweetLikesByUser(UserDbEntry userDbEntry) throws SQLException {
        return getAllTweetLikesByUser(userDbEntry.getId());
    }

    public List<TweetLikeDbEntry> getAllTweetLikes() throws SQLException {
        List<TweetLikeDbEntry> tweetLikeDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id, tweet_id FROM tweet_likes")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetLikeDbEntries.add(new TweetLikeDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("user_id"))
                    );
                }
            }
        }
        return tweetLikeDbEntries;
    }

    public void insertTweetRetweet(TweetRetweetDbEntry tweetRetweetDbEntry) throws SQLException {
        insertTweetRetweets(Collections.singletonList(tweetRetweetDbEntry));
    }

    public void insertTweetRetweets(List<TweetRetweetDbEntry> tweetRetweetDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_retweets (tweet_id, user_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (TweetRetweetDbEntry tweetRetweetDbEntry : tweetRetweetDbEntries) {
                ps.setLong(1, tweetRetweetDbEntry.getTweetId());
                ps.setLong(2, tweetRetweetDbEntry.getUserId());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public boolean existsTweetRetweet(TweetDbEntry tweetDbEntry, UserDbEntry userDbEntry) throws SQLException {
        return existsTweetRetweet(tweetDbEntry.getId(), userDbEntry.getId());
    }

    public Optional<TweetRetweetDbEntry> getTweetRetweet(TweetDbEntry tweetDbEntry, UserDbEntry userDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_retweets WHERE tweet_id = ? AND user_id = ?")) {
            ps.setLong(1, tweetDbEntry.getId());
            ps.setLong(2, userDbEntry.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetRetweetDbEntry(
                            rs.getLong("id"),
                            tweetDbEntry.getId(),
                            userDbEntry.getId())
                    );
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetRetweetDbEntry> getAllTweetRetweetsByTweet(long retweetedTweetId) throws SQLException {
        List<TweetRetweetDbEntry> tweetRetweetDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id FROM tweet_retweets WHERE tweet_id = ?")) {
            ps.setLong(1, retweetedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetRetweetDbEntries.add(new TweetRetweetDbEntry(
                            rs.getLong("id"),
                            retweetedTweetId,
                            rs.getLong("user_id"))
                    );
                }
            }
        }
        return tweetRetweetDbEntries;
    }

    public List<TweetRetweetDbEntry> getAllTweetRetweetsByTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        return getAllTweetRetweetsByTweet(tweetDbEntry.getId());
    }

    public List<TweetRetweetDbEntry> getAllTweetRetweetsByUser(long userId) throws SQLException {
        List<TweetRetweetDbEntry> tweetRetweetDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, tweet_id FROM tweet_retweets WHERE user_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetRetweetDbEntries.add(new TweetRetweetDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            userId)
                    );
                }
            }
        }
        return tweetRetweetDbEntries;
    }

    public List<TweetRetweetDbEntry> getAllTweetRetweetsByUser(UserDbEntry userDbEntry) throws SQLException {
        return getAllTweetRetweetsByUser(userDbEntry.getId());
    }

    public List<TweetRetweetDbEntry> getAllTweetRetweets() throws SQLException {
        List<TweetRetweetDbEntry> tweetRetweetDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, user_id, tweet_id FROM tweet_retweets")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetRetweetDbEntries.add(new TweetRetweetDbEntry(
                            rs.getLong("id"),
                            rs.getLong("tweet_id"),
                            rs.getLong("user_id"))
                    );
                }
            }
        }
        return tweetRetweetDbEntries;
    }

    public void insertTweetReply(TweetReplyDbEntry tweetReplyDbEntry) throws SQLException {
        insertTweetReplies(Collections.singletonList(tweetReplyDbEntry));
    }

    public void insertTweetReplies(List<TweetReplyDbEntry> tweetReplyDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_replies (reply_tweet_id, replied_tweet_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (TweetReplyDbEntry tweetReplyDbEntry : tweetReplyDbEntries) {
                ps.setLong(1, tweetReplyDbEntry.getReplyTweetId());
                ps.setLong(2, tweetReplyDbEntry.getRepliedTweetId());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public boolean existsTweetReply(TweetDbEntry tweetDbEntry, TweetDbEntry replyTweetDbEntry) throws SQLException {
        return existsTweetReply(tweetDbEntry.getId(), replyTweetDbEntry.getId());
    }

    public Optional<TweetReplyDbEntry> getTweetReply(TweetDbEntry tweetDbEntry, TweetDbEntry replyTweetDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_replies WHERE reply_tweet_id = ? AND replied_tweet_id = ?")) {
            ps.setLong(1, replyTweetDbEntry.getId());
            ps.setLong(2, tweetDbEntry.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetReplyDbEntry(
                            rs.getLong("id"),
                            replyTweetDbEntry.getId(),
                            tweetDbEntry.getId()));
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetReplyDbEntry> getTweetRepliesByUser(long userId) throws SQLException {
        List<TweetReplyDbEntry> tweetReplies = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, reply_tweet_id, replied_tweet_id FROM tweet_replies INNER JOIN tweets ON reply_tweet_id = tweets.id WHERE tweets.author_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReplies.add(new TweetReplyDbEntry(
                            rs.getLong("id"),
                            rs.getLong("reply_tweet_id"),
                            rs.getLong("replied_tweet_id"))
                    );
                }
            }
        }
        return tweetReplies;
    }

    public List<TweetReplyDbEntry> getTweetRepliesByUser(UserDbEntry userDbEntry) throws SQLException {
        return getTweetRepliesByUser(userDbEntry.getId());
    }

    public List<TweetReplyDbEntry> getTweetRepliesByUserAndRepliedTweet(long userId, long repliedTweetId) throws SQLException {
        List<TweetReplyDbEntry> tweetReplies = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, reply_tweet_id, replied_tweet_id FROM tweet_replies INNER JOIN tweets ON reply_tweet_id = tweets.id WHERE tweets.author_id = ? AND replied_tweet_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, repliedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReplies.add(new TweetReplyDbEntry(
                            rs.getLong("id"),
                            rs.getLong("reply_tweet_id"),
                            rs.getLong("replied_tweet_id"))
                    );
                }
            }
        }
        return tweetReplies;
    }

    public List<TweetReplyDbEntry> getTweetRepliesByRepliedTweet(long repliedTweetId) throws SQLException {
        List<TweetReplyDbEntry> tweetReplies = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, reply_tweet_id, replied_tweet_id FROM tweet_replies WHERE replied_tweet_id = ?")) {
            ps.setLong(1, repliedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetReplies.add(new TweetReplyDbEntry(
                            rs.getLong("id"),
                            rs.getLong("reply_tweet_id"),
                            rs.getLong("replied_tweet_id"))
                    );
                }
            }
        }
        return tweetReplies;
    }

    public List<TweetReplyDbEntry> getTweetRepliesByRepliedTweet(TweetDbEntry tweetDbEntry) throws SQLException {
        return getTweetRepliesByRepliedTweet(tweetDbEntry.getId());
    }

    public void insertTweetQuote(TweetQuoteDbEntry tweetQuoteDbEntry) throws SQLException {
        insertTweetQuotes(Collections.singletonList(tweetQuoteDbEntry));
    }

    public void insertTweetQuotes(List<TweetQuoteDbEntry> tweetQuoteDbEntries) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO tweet_quotes (quote_tweet_id, quoted_tweet_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (TweetQuoteDbEntry tweetQuoteDbEntry : tweetQuoteDbEntries) {
                ps.setLong(1, tweetQuoteDbEntry.getQuoteTweetId());
                ps.setLong(2, tweetQuoteDbEntry.getQuotedTweetId());
                ps.addBatch();
            }
            ps.executeBatch();
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

    public boolean existsTweetQuote(TweetDbEntry tweetDbEntry, TweetDbEntry quoteTweetDbEntry) throws SQLException {
        return existsTweetQuote(tweetDbEntry.getId(), quoteTweetDbEntry.getId());
    }

    public Optional<TweetQuoteDbEntry> getTweetQuote(TweetDbEntry tweetDbEntry, TweetDbEntry quoteTweetDbEntry) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id FROM tweet_quotes WHERE quote_tweet_id = ? AND quoted_tweet_id = ?")) {
            ps.setLong(1, quoteTweetDbEntry.getId());
            ps.setLong(2, tweetDbEntry.getId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TweetQuoteDbEntry(
                            rs.getLong("id"),
                            quoteTweetDbEntry.getId(),
                            tweetDbEntry.getId())
                    );
                }
            }
        }
        return Optional.empty();
    }

    public List<TweetQuoteDbEntry> getTweetQuotesByUser(long userId) throws SQLException {
        List<TweetQuoteDbEntry> tweetQuoteDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, quote_tweet_id, quoted_tweet_id FROM tweet_quotes INNER JOIN tweets ON quote_tweet_id = tweets.id WHERE tweets.author_id = ?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetQuoteDbEntries.add(new TweetQuoteDbEntry(
                            rs.getLong("id"),
                            rs.getLong("quote_tweet_id"),
                            rs.getLong("quoted_tweet_id"))
                    );
                }
            }
        }
        return tweetQuoteDbEntries;
    }

    public List<TweetQuoteDbEntry> getTweetQuotesByUser(UserDbEntry userDbEntry) throws SQLException {
        return getTweetQuotesByUser(userDbEntry.getId());
    }

    public List<TweetQuoteDbEntry> getTweetQuotesByUserAndQuotedTweet(long userId, long quotedTweetId) throws SQLException {
        List<TweetQuoteDbEntry> tweetQuoteDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, quote_tweet_id, quoted_tweet_id FROM tweet_quotes INNER JOIN tweets ON quote_tweet_id = tweets.id WHERE tweets.author_id = ? AND quoted_tweet_id = ?")) {
            ps.setLong(1, userId);
            ps.setLong(2, quotedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetQuoteDbEntries.add(new TweetQuoteDbEntry(
                            rs.getLong("id"),
                            rs.getLong("quote_tweet_id"),
                            rs.getLong("quoted_tweet_id"))
                    );
                }
            }
        }
        return tweetQuoteDbEntries;
    }

    public List<TweetQuoteDbEntry> getTweetQuotesByQuotedTweet(long quotedTweetId) throws SQLException {
        List<TweetQuoteDbEntry> tweetQuoteDbEntries = new ArrayList<>();
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
                "SELECT id, quote_tweet_id, quoted_tweet_id FROM tweet_quotes WHERE quoted_tweet_id = ?")) {
            ps.setLong(1, quotedTweetId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tweetQuoteDbEntries.add(new TweetQuoteDbEntry(
                            rs.getLong("id"),
                            rs.getLong("quote_tweet_id"),
                            rs.getLong("quoted_tweet_id"))
                    );
                }
            }
        }
        return tweetQuoteDbEntries;
    }

    public List<TweetQuoteDbEntry> getTweetQuotesByQuotedTweet(TweetDbEntry tweet) throws SQLException {
        return getTweetQuotesByQuotedTweet(tweet.getId());
    }
}
