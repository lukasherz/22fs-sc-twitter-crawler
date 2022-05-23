package de.lukasherz.twittercrawler.data.entities.tweets.contextannotation;

import com.twitter.clientlib.model.ContextAnnotationDomainFields;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class ContextAnnotationDomainDbEntry {
    private long id;
    private String name;
    private String description;

    public static ContextAnnotationDomainDbEntry parse(ContextAnnotationDomainFields domain) {
        return ContextAnnotationDomainDbEntry.builder()
                .id(Long.parseLong(domain.getId()))
                .name(domain.getName())
                .description(domain.getDescription())
                .build();
    }

}