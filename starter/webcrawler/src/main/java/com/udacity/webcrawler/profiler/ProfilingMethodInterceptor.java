package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

    private final Clock clock;
    private final Object target;
    private final ProfilingState state;
    private final ZonedDateTime timeStart;

    // TODO: You will need to add more instance fields and constructor arguments to this class.
    public ProfilingMethodInterceptor(Clock clock, Object target, ProfilingState state, ZonedDateTime timeStart) {
        this.clock = clock;
        this.target = target;
        this.state = state;
        this.timeStart = timeStart;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // TODO: This method interceptor should inspect the called method to see if it is a profiled
        //       method. For profiled methods, the interceptor should record the start time, then
        //       invoke the method using the object that is being profiled. Finally, for profiled
        //       methods, the interceptor should record how long the method call took, using the
        //       ProfilingState methods.
        Instant startTime = null;
        Object invoked;
        if (method.isAnnotationPresent(Profiled.class)) {
            startTime = clock.instant();
        }
        try {
            invoked = method.invoke(target, args);
        } catch (InvocationTargetException ex) {
            throw ex.getTargetException();
        } catch (IllegalAccessException ex) {
            throw new RuntimeException();
        } finally {
            if (method.isAnnotationPresent(Profiled.class)) {
                Duration duration = null;
                if (startTime != null) {
                    duration = Duration.between(startTime, clock.instant());
                }
                state.record(target.getClass(), method, duration);
            }
        }
        return invoked;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProfilingMethodInterceptor that)) return false;
        return Objects.equals(clock, that.clock) && Objects.equals(target, that.target) && Objects.equals(state, that.state) && Objects.equals(timeStart, that.timeStart);
    }
}
