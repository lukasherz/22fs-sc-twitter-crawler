package de.lukasherz.twittercrawler.data.entities.tweets.contextannotation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class ContextAnnotationDomain {
    private long id;
    private String name;
    private String description;

}