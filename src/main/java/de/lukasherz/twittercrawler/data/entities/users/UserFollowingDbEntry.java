package de.lukasherz.twittercrawler.data.entities.users;

import lombok.*;

@Builder
@Data
@AllArgsConstructor
public class UserFollowingDbEntry {
    private Long id;
    private long userId;
    private long followingId;

}