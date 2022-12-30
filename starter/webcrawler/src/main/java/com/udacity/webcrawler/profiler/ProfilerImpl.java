package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

    private final Clock clock;
    private final ProfilingState state = new ProfilingState();
    private final ZonedDateTime startTime;

    @Inject
    ProfilerImpl(Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        this.startTime = ZonedDateTime.now(clock);
    }

    @Override
    public <T> T wrap(Class<T> klass, T delegate) {
        Objects.requireNonNull(klass);

        // TODO: Use a dynamic proxy (java.lang.reflect.Proxy) to "wrap" the delegate in a
        //       ProfilingMethodInterceptor and return a dynamic proxy from this method.
        //       See https://docs.oracle.com/javase/10/docs/api/java/lang/reflect/Proxy.html.
        boolean isProfiled = false;
        for (Method method : klass.getDeclaredMethods()) {
            if (method.getAnnotation(Profiled.class) != null) {
                isProfiled = true;
                break;
            }
        }
        if (!isProfiled) {
            throw new IllegalArgumentException("Not profiled!");
        }
        ProfilingMethodInterceptor handler = new ProfilingMethodInterceptor(clock, delegate, state, startTime);
        @SuppressWarnings("unchecked")
        T f = (T) Proxy.newProxyInstance(ProfilerImpl.class.getClassLoader(),
                new Class[]{klass},
                handler);

        return f;
    }

    @Override
    public void writeData(Path path) throws IOException {
        // TODO: Write the ProfilingState data to the given file path. If a file already exists at that
        //       path, the new data should be appended to the existing file.
        Objects.requireNonNull(path);
        if (Files.notExists(path)) {
            Files.createFile(path);
        } else {
            try (BufferedWriter writer = Files.newBufferedWriter(path)) {
                writeData(writer);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void writeData(Writer writer) throws IOException {
        writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
        writer.write(System.lineSeparator());
        state.write(writer);
        writer.write(System.lineSeparator());
    }
}
