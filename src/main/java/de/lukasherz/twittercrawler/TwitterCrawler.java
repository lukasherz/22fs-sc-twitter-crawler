package de.lukasherz.twittercrawler;

import com.google.common.collect.ImmutableList;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import java.util.Arrays;

public class TwitterCrawler {

    private static final ImmutableList<String> TOKENS = ImmutableList.of(
        "AAAAAAAAAAAAAAAAAAAAAAnqcQEAAAAAvCh8TM%2FpzS3pnvFL%2B9eraD5LJNo%3D1cAwKaKCB8hbbJAK4TtMX0YzyFv77CSiDHJYry5jrJ8V916ZFA",
        "AAAAAAAAAAAAAAAAAAAAAKXrcQEAAAAAEFV%2BFRwZ%2Bs1yk6jWKjkqANvM0%2F0%3DpuMWkXJMkJuxsAMh94QlwlABMvdwffLWS3fD1HCksaR18ARaRb",
        "AAAAAAAAAAAAAAAAAAAAAGLrcQEAAAAAXVPWeR34g3%2FVBOzrwJd54%2FH5oAo%3Df1nMWHK1c8YDgjVxC16ihfnRlJQ3KfnGwkKO72aSCAbwJdlY4f",
        "AAAAAAAAAAAAAAAAAAAAAPY%2FdAEAAAAAzTUMKlJ3XdIQVdpXyDWdEEpvJW0%3D8db8tEH2aD0wBQruP1WWK0InvjEuWqzlX0acYomrPwZYlyr1De",
        "AAAAAAAAAAAAAAAAAAAAAA5AdAEAAAAAMQbHyTVOAX4O9sbExafEy3Sm5CE%3D8wM1PHiGHGb9DUNrbQWrFHzHefSyKAydKoXc0VpesFO1VgQW1L",
        "AAAAAAAAAAAAAAAAAAAAABRAdAEAAAAAw5CqUDE10gkniSlCI5PNlpXgiAY%3DZtvk70gXGW2pOi8CDzfqCoQpjYk1A1RRMvCuguarAAitYfcBwo",
        "AAAAAAAAAAAAAAAAAAAAAClAdAEAAAAAavt12vQPaASwCHnq6CZwAJXGS2I%3DJh4NKGKchAtgZg67NA4e8qs0A9RaWd9PXpqL9nHWdvJAF1KAYP",
        "AAAAAAAAAAAAAAAAAAAAAHNAdAEAAAAAYZEMva804t85z0bI0dK5V3nNYqA%3DBL9Jfk7f7Ywohzv2YEbbqzNhtZ3juG4GnJoKkBFPJ7uofKyYgJ",
        "AAAAAAAAAAAAAAAAAAAAAJZAdAEAAAAAO6JWWGvkW3ovZ8RCb6o4LC%2BdOUw%3DJIHDrcnBp95NDpCm70j0EEku1M0f9EPNtE3WIuwEX4VfFpg0tH"
    );
    public static String TOKEN = "";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println(
                "Usage: java -jar TwitterCrawler.jar <token_id> <count_of-tweets_per_hashtag> <#hashtags...>");
            return;
        }

        TOKEN = TOKENS.get(Integer.parseInt(args[0]));

        CrawlerHandler crawlerHandler = CrawlerHandler.getInstance();

        for (String arg : Arrays.stream(args).skip(2).toList()) {
            crawlerHandler.addHashtagSearchToQuery(arg, Integer.parseInt(args[1]));
        }

        crawlerHandler.startSchedulers();
    }
}
