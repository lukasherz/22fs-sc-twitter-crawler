package de.lukasherz.twittercrawler;

import de.lukasherz.twittercrawler.crawler.CrawlerHandler;

public class TwitterCrawler {

    public static void main(String[] args) {
        new CrawlerHandler().startSchedulers();

//        TwitterApi api = new TwitterApi();
//        api.setTwitterCredentials(new TwitterCredentialsBearer("AAAAAAAAAAAAAAAAAAAAAAnqcQEAAAAAvCh8TM%2FpzS3pnvFL%2B9eraD5LJNo%3D1cAwKaKCB8hbbJAK4TtMX0YzyFv77CSiDHJYry5jrJ8V916ZFA"));
//
//        List<Tweet> data;
//
//        try {
//            data = api.tweets().tweetsRecentSearch(
//                    "#trump -is:retweet -is:reply -is:quote lang:en",
//                    null,
//                    OffsetDateTime.now().minus(1, ChronoUnit.DAYS),
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null).getData();
//        } catch (ApiException e) {
//            throw new RuntimeException(e);
//        }
//
//        if (data == null) return;
//
//        for (Tweet tweet : data) {
//            try {
//                System.out.println("--------------START---------------");
//                SingleTweetLookupResponse tweetById = api.tweets().findTweetById(tweet.getId(), null, Set.of("context_annotations", "entities", "geo"), null, null, null, null);
//                System.out.println(tweetById.getData().getText());
//                System.out.println(tweetById.getData().getGeo());
//                System.out.println(tweetById.getData().getId());
//                List<ContextAnnotation> contextAnnotations = tweetById.getData().getContextAnnotations();
//                if (contextAnnotations != null) {
//                    contextAnnotations.stream().map(ContextAnnotation::toString).forEach(System.out::println);
//                }
//                System.out.println("https://twitter.com/random/status/" + tweetById.getData().getId());
//                System.out.println("---------------END----------------\n\n");
//            } catch (ApiException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }
}
