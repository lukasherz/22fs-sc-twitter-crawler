package de.lukasherz.twittercrawler.data.entities.users;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Builder
@Data
@AllArgsConstructor
public class User {
    private long id;
    private Instant creationDate;
    private String username;
    private String name;
    private boolean verified;
    private String profilePictureUrl;
    private String location;
    private String url;
    private String biography;
}