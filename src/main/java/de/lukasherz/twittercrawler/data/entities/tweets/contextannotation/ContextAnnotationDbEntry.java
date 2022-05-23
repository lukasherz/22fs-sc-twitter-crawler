package de.lukasherz.twittercrawler.data.entities.tweets.contextannotation;

import com.twitter.clientlib.model.ContextAnnotation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class ContextAnnotationDbEntry {
    private Long id;
    private long contextAnnotationDomainId;
    private long contextAnnotationEntityId;

    public static ContextAnnotationDbEntry parse(ContextAnnotation entity) {
        return ContextAnnotationDbEntry.builder()
                .contextAnnotationDomainId(Long.parseLong(entity.getDomain().getId()))
                .contextAnnotationEntityId(Long.parseLong(entity.getEntity().getId()))
                .build();
    }

}