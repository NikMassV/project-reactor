package edu.mikita;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StreamTest {

    private final Predicate<Integer> checker = num -> {
        for (int i = 2; i < num; i++) {
            if (num % i == 0) {
                return false;
            }
        }
        return true;
    };

    @Test
    public void reactorStream() {
        Integer[] array = generateIntArray(100);
        Flux.fromArray(array)
                .filter(i -> i % 2 != 0)
                .map(i -> "Number " + i + " is prime: " + checker.test(i))
                .subscribe(System.out::println);
    }

    @Test
    public void syncStream() {
        Flux.<Integer>create(s -> {
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.complete();
                }).map(this::generateIntArray)
                .map(array -> Arrays.stream(array)
                        .filter(i -> (i % 2 != 0))
                        .collect(Collectors.toList()))
                .flatMap(list -> Flux.fromIterable(list)
                        .map(i -> "Number " + i + " is prime: " + checker.test(i)))
                .subscribe(result -> System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> " + result));
    }

    @Test
    public void asyncStream_subscribeOn_withoutAwait() {
        Flux.<Integer>create(s -> {
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.complete();
                }).map(this::generateIntArray)
                .map(array -> Arrays.stream(array)
                        .filter(i -> (i % 2 != 0))
                        .collect(Collectors.toList()))
                .flatMap(list -> Flux.fromIterable(list)
                        .map(i -> "Number " + i + " is prime: " + checker.test(i)))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(result -> System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> " + result));
    }

    @Test
    public void asyncStream_subscribeOn_withAwait() throws InterruptedException {
        var cdl = new CountDownLatch(1);
        Flux.<Integer>create(s -> {
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.complete();
                }).map(this::generateIntArray)
                .map(array -> Arrays.stream(array)
                        .filter(i -> (i % 2 != 0))
                        .collect(Collectors.toList()))
                .flatMap(list -> Flux.fromIterable(list)
                        .map(i -> "Number " + i + " is prime: " + checker.test(i)))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(ignore -> cdl.countDown())
                .subscribe(result -> System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> " + result));
        cdl.await();
    }

    @Test
    public void asyncStream_publishOn_withAwait() throws InterruptedException {
        var cdl = new CountDownLatch(1);

        Flux.<Integer>create(s -> {
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.complete();
                }).map(this::generateIntArray)
                .map(array -> {
                    System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> filtering array with size: " + array.length);
                    return Arrays.stream(array)
                            .filter(i -> (i % 2 != 0))
                            .collect(Collectors.toList());
                })
                .publishOn(Schedulers.boundedElastic())
                .flatMap(list -> Flux.fromIterable(list)
                        .map(i -> "Number " + i + " is prime: " + checker.test(i)))
                .doFinally(ignore -> cdl.countDown())
                .subscribe(result -> System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> " + result));

        cdl.await();
    }

    @Test
    public void researchReactor_publishOn_innerFlux() throws InterruptedException {
        var cdl = new CountDownLatch(1);

        Flux.<Integer>create(s -> {
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.complete();
                }).map(this::generateIntArray)
                .map(array -> {
                    System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> filtering array with size: " + array.length);
                    return Arrays.stream(array)
                            .filter(i -> (i % 2 != 0))
                            .collect(Collectors.toList());
                })
                .flatMap(list -> Flux.fromIterable(list)
                        .publishOn(Schedulers.boundedElastic())
                        .map(i -> "Number " + i + " is prime: " + checker.test(i)))
                .doFinally(ignore -> cdl.countDown())
                .subscribe(result -> System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> " + result));

        cdl.await();
    }

    @Test
    public void researchReactor_subscribeOn_perElement() throws InterruptedException {
        var cdl = new CountDownLatch(1);

        Flux.<Integer>create(s -> {
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(50);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(100);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.next(150);
                    s.complete();
                }).map(this::generateIntArray)
                .map(array -> {
                    System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> filtering array with size: " + array.length);
                    return Arrays.stream(array)
                            .filter(i -> (i % 2 != 0))
                            .collect(Collectors.toList());
                })
                .flatMap(list -> Flux.fromIterable(list)
                        .flatMap(i -> Mono.defer(() -> Mono.just("Number " + i + " is prime: " + checker.test(i)))
                                .subscribeOn(Schedulers.boundedElastic()))
                )
                .doFinally(ignore -> cdl.countDown())
                .subscribe(result -> System.out.println("IN thread [" + Thread.currentThread().getName() + "] -> " + result));

        cdl.await();
    }

    @Test
    public void hotStream() throws InterruptedException {
        var cdl = new CountDownLatch(1);
        Flux<Object> stream = Flux.create(fluxSink -> {
                    while (true) {
                        fluxSink.next(System.currentTimeMillis());
                    }
                })
                .sample(Duration.ofMillis(500))
                .doFinally(ignore -> cdl.countDown())
                .subscribeOn(Schedulers.single())
                .share();
        stream.subscribe(o -> System.out.println("[" + Thread.currentThread().getName() + "] Subscriber 1 -> " + o));
        Thread.sleep(4000);
        stream.subscribe(o -> System.out.println("[" + Thread.currentThread().getName() + "] Subscriber 2 -> " + o));
        cdl.await();
    }

    @Test
    public void reactorContext_viaSubscribe() {
        String ctxKey = "key";
        Flux.fromArray(generateIntArray(10))
                .flatMap(i -> Mono.deferContextual(ctx -> {
                            int value = ctx.<Integer>getOrEmpty(ctxKey).orElseThrow(() -> new IllegalArgumentException("Ctx key not found!"));
                            String result = i % value == 0
                                    ? String.format("Thread [%s] -> %d divisor of the number %d", Thread.currentThread().getName(), value, i)
                                    : String.format("Thread [%s] -> %d NOT divisor of the number %d", Thread.currentThread().getName(), value, i);

                            return Mono.just(result);
                        })
                ).subscribe(System.out::println,
                        null,
                        null,
                        Context.of(ctxKey, ThreadLocalRandom.current().nextInt(2, 10)));
    }

    @Test
    public void reactorContext_viaContextWriteLambda() {
        String ctxKey = "key";
        Flux.fromArray(generateIntArray(10))
                .flatMap(i -> Mono.deferContextual(ctx -> {
                            int value = ctx.<Integer>getOrEmpty(ctxKey).orElseThrow(() -> new IllegalArgumentException("Ctx key not found!"));
                            String result = i % value == 0
                                    ? String.format("Thread [%s] -> %d divisor of the number %d", Thread.currentThread().getName(), value, i)
                                    : String.format("Thread [%s] -> %d NOT divisor of the number %d", Thread.currentThread().getName(), value, i);

                            return Mono.just(result);
                        })
                ).contextWrite(ctx -> ctx.put(ctxKey, ThreadLocalRandom.current().nextInt(2, 10)))
                .subscribe(System.out::println);
    }

    @Test
    public void reactorContext_viaContextWriteWithContext() {
        String ctxKey = "key";
        Flux.fromArray(generateIntArray(10))
                .flatMap(i -> Mono.deferContextual(ctx -> {
                            int value = ctx.<Integer>getOrEmpty(ctxKey).orElseThrow(() -> new IllegalArgumentException("Ctx key not found!"));
                            String result = i % value == 0
                                    ? String.format("Thread [%s] -> %d divisor of the number %d", Thread.currentThread().getName(), value, i)
                                    : String.format("Thread [%s] -> %d NOT divisor of the number %d", Thread.currentThread().getName(), value, i);

                            return Mono.just(result);
                        })
                ).contextWrite(Context.of(ctxKey, ThreadLocalRandom.current().nextInt(2, 10)))
                .subscribe(System.out::println);
    }

    private Integer[] generateIntArray(int size) {
        Integer[] array = new Integer[size];
        for (int i = 0; i < size; i++) {
            array[i] = ThreadLocalRandom.current().nextInt(90_000_000, 100_000_000);
        }
        return array;
    }
}
