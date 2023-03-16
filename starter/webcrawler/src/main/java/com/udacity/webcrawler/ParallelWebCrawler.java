package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
    public static Lock lock = new ReentrantLock();
    private final Clock clock;
    private final PageParserFactory parserFactory;
    private final Duration timeout;
    private final int popularWordCount;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;
    private final ForkJoinPool pool;

    @Inject
    ParallelWebCrawler(
            Clock clock,
            PageParserFactory parserFactory,
            @Timeout Duration timeout,
            @PopularWordCount int popularWordCount,
            @MaxDepth int maxDepth,
            @IgnoredUrls List<Pattern> ignoredUrls,
            @TargetParallelism int threadCount) {
        this.clock = clock;
        this.parserFactory = parserFactory;
        this.timeout = timeout;
        this.popularWordCount = popularWordCount;
        this.maxDepth = maxDepth;
        this.ignoredUrls = ignoredUrls;
        this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    }

    @Override
    public CrawlResult crawl(List<String> startingUrls) {
        Instant deadline = clock.instant().plus(timeout);
        ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
        ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
        for (String url : startingUrls) {
            pool.invoke(new ParallelCrawlInternal(url, deadline, maxDepth, counts, visitedUrls, parserFactory, clock, ignoredUrls));
        }

        if (counts.isEmpty()) {
            return new CrawlResult.Builder()
                    .setWordCounts(counts)
                    .setUrlsVisited(visitedUrls.size())
                    .build();
        }

        return new CrawlResult.Builder()
                .setWordCounts(WordCounts.sort(counts, popularWordCount))
                .setUrlsVisited(visitedUrls.size())
                .build();
    }

    @Override
    public int getMaxParallelism() {
        return Runtime.getRuntime().availableProcessors();
    }

    private static class ParallelCrawlInternal extends RecursiveAction {

        private final String url;
        private final Instant deadline;
        private final int maxDepth;
        private final ConcurrentMap<String, Integer> counts;
        private final ConcurrentSkipListSet<String> visitedUrls;
        private final PageParserFactory parserFactory;
        private final Clock clock;
        private final List<Pattern> ignoredUrls;

        public ParallelCrawlInternal(String url,
                                     Instant deadline,
                                     int maxDepth,
                                     ConcurrentMap<String, Integer> counts,
                                     ConcurrentSkipListSet<String> visitedUrls,
                                     PageParserFactory parserFactory,
                                     Clock clock,
                                     List<Pattern> ignoredUrls) {
            this.url = url;
            this.deadline = deadline;
            this.maxDepth = maxDepth;
            this.counts = counts;
            this.visitedUrls = visitedUrls;
            this.parserFactory = parserFactory;
            this.clock = clock;
            this.ignoredUrls = ignoredUrls;
        }


        @Override
        protected void compute() {
            if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
                return;
            }
            for (Pattern pattern : ignoredUrls) {
                if (pattern.matcher(url).matches()) {
                    return;
                }
            }
            try {
                lock.lock();
                if (visitedUrls.contains(url)) {
                    return;
                }
                visitedUrls.add(url);
            } finally {
                lock.unlock();
            }

            PageParser.Result result = parserFactory.get(url).parse();
            try {
                lock.lock();
                for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
                    if (counts.containsKey(e.getKey())) {
                        counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
                    } else {
                        counts.put(e.getKey(), e.getValue());
                    }
                }
            } finally {
                lock.unlock();
            }

            List<ParallelCrawlInternal> subtasks = new ArrayList<>();
            for (String url : result.getLinks()) {
                subtasks.add(new ParallelCrawlInternal(url, deadline, maxDepth - 1, counts, visitedUrls, parserFactory, clock, ignoredUrls));
            }
            subtasks.forEach(ForkJoinTask::invoke);
        }
    }
}
