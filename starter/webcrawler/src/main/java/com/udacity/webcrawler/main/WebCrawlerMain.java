package com.udacity.webcrawler.main;

import com.google.inject.Guice;
import com.udacity.webcrawler.WebCrawler;
import com.udacity.webcrawler.WebCrawlerModule;
import com.udacity.webcrawler.json.ConfigurationLoader;
import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.json.CrawlResultWriter;
import com.udacity.webcrawler.json.CrawlerConfiguration;
import com.udacity.webcrawler.profiler.Profiler;
import com.udacity.webcrawler.profiler.ProfilerModule;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Objects;

public final class WebCrawlerMain {

    private final CrawlerConfiguration config;
    @Inject
    private WebCrawler crawler;
    @Inject
    private Profiler profiler;

    private WebCrawlerMain(CrawlerConfiguration config) {
        this.config = Objects.requireNonNull(config);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: WebCrawlerMain [starting-url]");
            return;
        }

        CrawlerConfiguration config = new ConfigurationLoader(Path.of(args[0])).load();
        new WebCrawlerMain(config).run();
    }

    private void run() throws IOException {
        Guice.createInjector(new WebCrawlerModule(config), new ProfilerModule()).injectMembers(this);

        CrawlResult result = crawler.crawl(config.getStartPages());
        CrawlResultWriter resultWriter = new CrawlResultWriter(result);
        // TODO: Write the crawl results to a JSON file (or System.out if the file name is empty)
        if (config.getResultPath().isEmpty()) {
            try (Writer writer = new OutputStreamWriter(System.out)) {
                resultWriter.write(writer);
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            Path path = Path.of(config.getResultPath());
            resultWriter.write(path);
        }

        // TODO: Write the profile data to a text file (or System.out if the file name is empty)
        if (config.getProfileOutputPath().isEmpty()) {
            try (Writer writer = new OutputStreamWriter(System.out)) {
                profiler.writeData(writer);
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            Path path = Path.of(config.getProfileOutputPath());
            profiler.writeData(path);
        }
    }
}
