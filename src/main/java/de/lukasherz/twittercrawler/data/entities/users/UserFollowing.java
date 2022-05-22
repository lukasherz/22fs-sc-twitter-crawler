package de.lukasherz.twittercrawler.data.entities.users;

import lombok.*;
import org.jetbrains.annotations.Nullable;

@Builder
@Data
@AllArgsConstructor
public class UserFollowing {
    private long id;
    private User user;
    private User following;

}