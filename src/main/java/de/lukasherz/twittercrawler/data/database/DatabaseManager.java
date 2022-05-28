package de.lukasherz.twittercrawler.data.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.lukasherz.twittercrawler.data.entities.tweets.TweetDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationDomainDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.ContextAnnotationEntityDbEntry;
import de.lukasherz.twittercrawler.data.entities.tweets.contextannotation.TweetContextAnnotationDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserDbEntry;
import de.lukasherz.twittercrawler.data.entities.users.UserFollowingDbEntry;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.flogger.Flogger;

@Flogger
public class DatabaseManager {

    private static DatabaseManager instance;
    private HikariDataSource hikariDataSource;

    private DatabaseManager() {
        log.atInfo().log("DatabaseManager starting...");

        hikariDataSource = new HikariDataSource(createHikariConfig());

        // init database
        try {
            initDatabase();
        } catch (SQLException e) {
            log.atSevere().withCause(e).log("could not init database");
        }

        log.atInfo().log("DatabaseManager started");
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
                log.atSevere().withCause(e).log("could not load config.properties");
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    log.atWarning().withCause(e).log("could not close config.properties");
                }
            }
        } else {
            log.atSevere().log("Could not load config.properties");
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.getProperty("jdbc.url"));
        config.setUsername(properties.getProperty("jdbc.username"));
        config.setPassword(properties.getProperty("jdbc.password"));
        config.addDataSourceProperty("useSSL", properties.getProperty("jdbc.useSSL"));
        config.addDataSourceProperty("serverTimezone", properties.getProperty("jdbc.serverTimezone"));
        config.addDataSourceProperty("allowPublicKeyRetrieval", properties.getProperty("jdbc.allowPublicKeyRetrieval"));
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(5);

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
                "    creation_date       datetime      NULL," +
                "    username            VARCHAR(15)   NOT NULL," +
                "    name                VARCHAR(50)   NOT NULL," +
                "    verified            BOOL          NULL," +
                "    profile_picture_url VARCHAR(1023) NULL," +
                "    location            VARCHAR(255)  NULL," +
                "    url                 VARCHAR(1023)  NULL," +
                "    biography           VARCHAR(1023)  NULL," +
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
                "    geo                   VARCHAR(1023)," +
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
            "CREATE TABLE IF NOT EXISTS users_pre_processed "
                + "( "
                + "    id                    BIGINT PRIMARY KEY, "
                + "    user_id               BIGINT, "
                + "    gender                INTEGER, "
                + "    political_affiliation INTEGER, "
                + "    democrat_following    INTEGER, "
                + "    republican_following  INTEGER, "
                + "    following_base        INTEGER, "
                + "    FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE "
                + ");")) {
            ps.execute();
        }

        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
            "CREATE OR REPLACE VIEW data_preprocessed AS "
                + "SELECT tweets.id                                 as tweet_id, "
                + "       tweets.author_id                          as user_id, "
                + "       tweets.text                               as tweet_text, "
                + "       tweets.created_at                         as tweet_created_at, "
                + "       tweets.search_query                       as tweet_search_query, "
                + "       tweets.metrics_like_count                 as tweet_metrics_like_count, "
                + "       tweets.metrics_quote_count                as tweet_metrics_quote_count, "
                + "       tweets.metrics_reply_count                as tweet_metrics_reply_count, "
                + "       tweets.metrics_retweet_count              as tweet_metrics_retweet_count, "
                + "       tweets.lang                               as tweet_lang, "
                + "       users.creation_date                       as user_creation_date, "
                + "       users.username                            as user_username, "
                + "       users.name                                as user_name, "
                + "       users.verified                            as user_verified, "
                + "       users.profile_picture_url                 as user_profile_picture_url, "
                + "       users.location                            as user_location, "
                + "       users.url                                 as user_url, "
                + "       users.biography                           as user_biography, "
                + "       users_pre_processed.gender                as user_gender, "
                + "       users_pre_processed.political_affiliation as user_political_affiliation, "
                + "       users_pre_processed.democrat_following    as user_democrat_following, "
                + "       users_pre_processed.republican_following  as user_republican_following, "
                + "       users_pre_processed.following_base        as user_following_count "
                + "FROM tweets "
                + "         INNER JOIN users ON tweets.author_id = users.id "
                + "         INNER JOIN users_pre_processed ON users.id = users_pre_processed.user_id "
                + "WHERE users_pre_processed.gender <> -1 "
                + "  AND users_pre_processed.political_affiliation <> -1;")) {
            ps.execute();
        }

//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "CREATE TABLE IF NOT EXISTS tweet_references_extern(" +
//                "    id                  BIGINT AUTO_INCREMENT," +
//                "    tweet_id            BIGINT      NOT NULL," +
//                "    referenced_tweet_id BIGINT      NOT NULL," +
//                "    reference_type      VARCHAR(15) NOT NULL," +
//                "    PRIMARY KEY (id)," +
//                "    FOREIGN KEY (tweet_id) REFERENCES tweets (id)," +
//                "    UNIQUE (tweet_id, referenced_tweet_id, reference_type)" +
//                ");")) {
//            ps.execute();
//        }
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
                ps.setTimestamp(2,
                    userDbEntry.getCreationDate() != null ? Timestamp.from(userDbEntry.getCreationDate()) : null);
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
        insertFollowings(userId, Collections.singletonList(followingId));
    }

    public void insertFollowings(long userId, List<Long> followingIds) throws SQLException {
        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
            "INSERT INTO user_following (user_id, following_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = id")) {
            for (long followingId : followingIds) {
                ps.setLong(1, userId);
                ps.setLong(2, followingId);

                ps.addBatch();
            }
            ps.executeBatch();
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

    public void insertTweetContextAnnotation(TweetContextAnnotationDbEntry tweetContextAnnotationDbEntry)
        throws SQLException {
        insertTweetContextAnnotations(Collections.singletonList(tweetContextAnnotationDbEntry));
    }

    public void insertTweetContextAnnotations(List<TweetContextAnnotationDbEntry> tweetContextAnnotationDbEntries)
        throws SQLException {
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

    public void insertTweetContextAnnotation(long tweetId, long contextAnnotationDomainId,
                                             long contextAnnotationEntityId) throws SQLException {
        insertTweetContextAnnotations(tweetId, Collections.singletonList(contextAnnotationDomainId),
            Collections.singletonList(contextAnnotationEntityId));
    }

    public void insertTweetContextAnnotations(long tweetId, List<Long> contextAnnotationDomainIds,
                                              List<Long> contextAnnotationEntityIds) throws SQLException {
        if (contextAnnotationDomainIds.size() != contextAnnotationEntityIds.size()) {
            throw new IllegalArgumentException(
                "contextAnnotationDomainIds and contextAnnotationEntityIds must have the same size");
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

    public boolean existsTweetContextAnnotation(TweetContextAnnotationDbEntry tweetContextAnnotationDbEntry)
        throws SQLException {
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

    public Optional<TweetContextAnnotationDbEntry> getTweetContextAnnotation(TweetDbEntry tweetDbEntry,
                                                                             ContextAnnotationDbEntry contextAnnotationDbEntry)
        throws SQLException {
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

    public List<TweetContextAnnotationDbEntry> getAllContextAnnotationsOfTweet(TweetDbEntry tweetDbEntry)
        throws SQLException {
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

    // no references exists because they are excluded in the search query
//    public void insertTweetReference(TweetReferenceDbEntry tweetReferenceDbEntry) throws SQLException {
//        insertTweetReferences(Collections.singletonList(tweetReferenceDbEntry));
//    }
//
//    public void insertTweetReferences(List<TweetReferenceDbEntry> tweetReferenceDbEntries) throws SQLException {
//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "INSERT INTO tweet_references_extern(tweet_id, referenced_tweet_id, reference_type) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = id")) {
//            for (TweetReferenceDbEntry tweetReferenceDbEntry : tweetReferenceDbEntries) {
//                ps.setLong(1, tweetReferenceDbEntry.getTweetId());
//                ps.setLong(2, tweetReferenceDbEntry.getReferencedTweetId());
//                ps.setString(3, tweetReferenceDbEntry.getReferenceType());
//                ps.addBatch();
//            }
//            ps.executeBatch();
//        }
//    }
//
//    public boolean existsTweetReference(long tweetId, long referencedTweetId) throws SQLException {
//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "SELECT id FROM tweet_references_extern WHERE tweet_id = ? AND referenced_tweet_id = ?")) {
//            ps.setLong(1, tweetId);
//            ps.setLong(2, referencedTweetId);
//            try (ResultSet rs = ps.executeQuery()) {
//                return rs.next();
//            }
//        }
//    }
//
//    public boolean existsTweetReference(TweetDbEntry tweetDbEntry, TweetDbEntry referencedTweetDbEntry)
//        throws SQLException {
//        return existsTweetReference(tweetDbEntry.getId(), referencedTweetDbEntry.getId());
//    }
//
//    public Optional<TweetReferenceDbEntry> getTweetReference(TweetDbEntry tweetDbEntry,
//                                                             TweetDbEntry referencedTweetDbEntry) throws SQLException {
//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_extern WHERE tweet_id = ? AND referenced_tweet_id = ?")) {
//            ps.setLong(1, tweetDbEntry.getId());
//            ps.setLong(2, referencedTweetDbEntry.getId());
//            try (ResultSet rs = ps.executeQuery()) {
//                if (rs.next()) {
//                    return Optional.of(new TweetReferenceDbEntry(
//                        rs.getLong("id"),
//                        rs.getLong("tweet_id"),
//                        rs.getLong("referenced_tweet_id"),
//                        rs.getString("reference_type")));
//                }
//            }
//        }
//        return Optional.empty();
//    }
//
//    public List<TweetReferenceDbEntry> getAllTweetReferencesByTweet(long tweetId) throws SQLException {
//        List<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_extern WHERE tweet_id = ?")) {
//            ps.setLong(1, tweetId);
//            try (ResultSet rs = ps.executeQuery()) {
//                while (rs.next()) {
//                    tweetReferenceDbEntries.add(new TweetReferenceDbEntry(
//                        rs.getLong("id"),
//                        rs.getLong("tweet_id"),
//                        rs.getLong("referenced_tweet_id"),
//                        rs.getString("reference_type")));
//                }
//            }
//        }
//        return tweetReferenceDbEntries;
//    }
//
//    public List<TweetReferenceDbEntry> getAllTweetReferencesByTweet(TweetDbEntry tweetDbEntry) throws SQLException {
//        return getAllTweetReferencesByTweet(tweetDbEntry.getId());
//    }
//
//    public List<TweetReferenceDbEntry> getAllTweetReferencesByReferencedTweet(long referencedTweetId)
//        throws SQLException {
//        List<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_extern WHERE referenced_tweet_id = ?")) {
//            ps.setLong(1, referencedTweetId);
//            try (ResultSet rs = ps.executeQuery()) {
//                while (rs.next()) {
//                    tweetReferenceDbEntries.add(new TweetReferenceDbEntry(
//                        rs.getLong("id"),
//                        rs.getLong("tweet_id"),
//                        rs.getLong("referenced_tweet_id"),
//                        rs.getString("reference_type")));
//                }
//            }
//        }
//        return tweetReferenceDbEntries;
//    }
//
//    public List<TweetReferenceDbEntry> getAllTweetReferencesByReferencedTweet(TweetDbEntry tweetDbEntry)
//        throws SQLException {
//        return getAllTweetReferencesByReferencedTweet(tweetDbEntry.getId());
//    }
//
//    public List<TweetReferenceDbEntry> getAllTweetReferences() throws SQLException {
//        List<TweetReferenceDbEntry> tweetReferenceDbEntries = new ArrayList<>();
//        try (Connection connection = getNewConnection(); PreparedStatement ps = connection.prepareStatement(
//            "SELECT id, tweet_id, referenced_tweet_id, reference_type FROM tweet_references_extern")) {
//            try (ResultSet rs = ps.executeQuery()) {
//                while (rs.next()) {
//                    tweetReferenceDbEntries.add(new TweetReferenceDbEntry(
//                        rs.getLong("id"),
//                        rs.getLong("tweet_id"),
//                        rs.getLong("referenced_tweet_id"),
//                        rs.getString("reference_type")));
//                }
//            }
//        }
//        return tweetReferenceDbEntries;
//    }
}
