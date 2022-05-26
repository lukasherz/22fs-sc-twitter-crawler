package de.lukasherz.twittercrawler.data.entities.users;

import com.twitter.clientlib.model.User;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Builder
@Data
@AllArgsConstructor
public class UserDbEntry {
    private long id;
    private Instant creationDate;
    private String username;
    private String name;
    private boolean verified;
    private String profilePictureUrl;
    private String location;
    private String url;
    private String biography;

    public static UserDbEntry parse(User user) {
        return UserDbEntry.builder()
                .id(Long.parseLong(user.getId()))
                .creationDate(user.getCreatedAt() != null ? user.getCreatedAt().toInstant() : null)
                .username(user.getUsername())
                .name(user.getName())
                .verified(Boolean.TRUE.equals(user.getVerified()))
                .profilePictureUrl(user.getProfileImageUrl() != null ? user.getProfileImageUrl().toString() : null)
                .location(user.getLocation())
                .url(user.getUrl())
                .biography(user.getDescription())
                .build();
    }
}