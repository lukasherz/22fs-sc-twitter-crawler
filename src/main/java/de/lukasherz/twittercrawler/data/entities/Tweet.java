package de.lukasherz.twittercrawler.data.entities;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Entity
@Table(name = "tweets")
public class Tweet {
    @Id
    @Column(name = "id", nullable = false)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "author_id", nullable = false)
    private User author;

    @Column(name = "text", nullable = false, length = 1023)
    private String text;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "metrics_retweet_count", nullable = false)
    private Integer metricsRetweetCount;

    @Column(name = "metrics_like_count", nullable = false)
    private Integer metricsLikeCount;

    @Column(name = "metrics_reply_count", nullable = false)
    private Integer metricsReplyCount;

    @Column(name = "metrics_quote_count", nullable = false)
    private Integer metricsQuoteCount;

    @Column(name = "lang", length = 15)
    private String lang;

    @Column(name = "geo")
    private String geo;

}