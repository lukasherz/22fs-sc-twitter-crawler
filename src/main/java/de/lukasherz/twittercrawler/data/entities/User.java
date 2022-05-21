package de.lukasherz.twittercrawler.data.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.Instant;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Entity
@Table(name = "users")
public class User {
    @Id
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "creation_date", nullable = false)
    private Instant creationDate;

    @Column(name = "username", nullable = false, length = 15)
    private String username;

    @Column(name = "name", nullable = false, length = 50)
    private String name;

    @Column(name = "verified", nullable = false)
    private Boolean verified = false;

    @Column(name = "profile_picture_url", length = 1023)
    private String profilePictureUrl;

    @Column(name = "location")
    private String location;

    @Column(name = "url")
    private String url;

    @Column(name = "biography", length = 160)
    private String biography;

}