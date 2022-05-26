package de.lukasherz.twittercrawler;

import com.google.common.collect.ImmutableList;
import de.lukasherz.twittercrawler.crawler.CrawlerHandler;
import java.util.Arrays;

public class TwitterCrawler {

    private static final ImmutableList<String> TOKENS = ImmutableList.of(
        "AAAAAAAAAAAAAAAAAAAAAAnqcQEAAAAAvCh8TM%2FpzS3pnvFL%2B9eraD5LJNo%3D1cAwKaKCB8hbbJAK4TtMX0YzyFv77CSiDHJYry5jrJ8V916ZFA",
        "AAAAAAAAAAAAAAAAAAAAAKXrcQEAAAAAEFV%2BFRwZ%2Bs1yk6jWKjkqANvM0%2F0%3DpuMWkXJMkJuxsAMh94QlwlABMvdwffLWS3fD1HCksaR18ARaRb",
        "AAAAAAAAAAAAAAAAAAAAAGLrcQEAAAAAXVPWeR34g3%2FVBOzrwJd54%2FH5oAo%3Df1nMWHK1c8YDgjVxC16ihfnRlJQ3KfnGwkKO72aSCAbwJdlY4f"
    );
    public static String TOKEN = "";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java -jar TwitterCrawler.jar <token_id> <count_of-tweets_per_hashtag> <#hashtags...>");
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
