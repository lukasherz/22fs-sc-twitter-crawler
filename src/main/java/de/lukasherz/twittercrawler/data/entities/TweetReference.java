package de.lukasherz.twittercrawler.data.entities;

import jakarta.persistence.*;
import lombok.*;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Entity
@Table(name = "tweet_references")
public class TweetReference {
    @Id
    @Column(name = "id", nullable = false)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "tweet_id", nullable = false)
    private Tweet tweet;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "referenced_tweet_id", nullable = false)
    private Tweet referencedTweet;

    @Column(name = "reference_type", nullable = false, length = 15)
    private String referenceType;

}