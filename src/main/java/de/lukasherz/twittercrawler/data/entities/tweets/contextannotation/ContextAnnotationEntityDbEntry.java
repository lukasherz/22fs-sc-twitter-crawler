package de.lukasherz.twittercrawler.data.entities.tweets.contextannotation;

import com.twitter.clientlib.model.ContextAnnotationEntityFields;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class ContextAnnotationEntityDbEntry {
    private long id;
    private String name;
    private String description;

    public static ContextAnnotationEntityDbEntry parse(ContextAnnotationEntityFields entity) {
        return ContextAnnotationEntityDbEntry.builder()
                .id(Long.parseLong(entity.getId()))
                .name(entity.getName())
                .description(entity.getDescription())
                .build();
    }

}