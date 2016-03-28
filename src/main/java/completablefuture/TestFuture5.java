package completablefuture;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * Simulator for couple of async tasks performed by CompletableFuture.<br/>
 * Requirements:
 * <ol>
 *     <li>
 *         If primary task finished with exception, the program should be halted.
 *     </li>
 *     <li>
 *         If secondary task finished with exception, it should be logged, the program proceed execution.
 *     </li>
 *     <li>
 *         If secondary task out of time, the main thread should be unblocked, the program proceed execution.
 *     </li>
 * </ol>
 * Tasks behavior controlled by global self-describing flags.
 *
 * @author Maxim Kolesnikov
 */
public class TestFuture5 {
    private static final boolean SHOULD_THROW_SECONDARY = false;
    private static final boolean SHOULD_THROW_PRIMARY = false;
    private static final boolean SIMULATE_TIME_OUT = false;

    private static final ScheduledExecutorService POOL = Executors.newScheduledThreadPool(10);

    public static void main(String[] args) throws InterruptedException, TimeoutException, ExecutionException {
        try {
            CompletableFuture<Optional<String>> secondaryFuture = supplyAsync(() -> {
                try {
                    return secondaryTask();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            CompletableFuture<Optional<String>> timedSecondaryFuture =
                    secondaryFuture.applyToEither(timeoutAfter(Duration.ofSeconds(10)), x -> x)
                                   .exceptionally((throwable) -> {
                                       System.err.println(
                                               "There was an error during the secondary task execution: " + throwable);
                                       return Optional.empty();
                                   });

            CompletableFuture<String> primaryFuture = supplyAsync(() -> {
                try {
                    return primaryTask();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            String primaryResult = primaryFuture.get();
            Optional<String> secondaryResult = timedSecondaryFuture.get();

            System.out.println("Final result: " + primaryResult +
                    (secondaryResult.isPresent() ? secondaryResult.get() : ""));
        } finally {
            POOL.shutdown();
        }
    }

    /**
     * Execute scheduled task which will immediately return promise with TimeoutException.
     * Used to control CompletableFuture timeouts in reactive style.
     * @param duration - sets timeout value.
     */
    private static <T> CompletableFuture<T> timeoutAfter(Duration duration) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        POOL.schedule(
                () -> promise.completeExceptionally(new TimeoutException("Timeout of operation")),
                duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    private static String primaryTask() throws InterruptedException {
        System.out.println("Run the primary async task");
        TimeUnit.SECONDS.sleep(1);
        if (SHOULD_THROW_PRIMARY) {
            throw new RuntimeException("Whoops, here is an error in the primary task");
        }
        TimeUnit.SECONDS.sleep(5);
        System.out.println("Primary async task is complete");
        return "Primary";
    }

    private static Optional<String> secondaryTask() throws InterruptedException {
        System.out.println("Run the secondary async task");
        TimeUnit.SECONDS.sleep(1);
        if (SHOULD_THROW_SECONDARY) {
            throw new RuntimeException("Whoops, here is an error in the secondary task");
        }
        TimeUnit.SECONDS.sleep(7);
        if (SIMULATE_TIME_OUT) {
            TimeUnit.SECONDS.sleep(5);
        }
        System.out.println("Secondary async task is complete");
        return Optional.of("Secondary");
    }
}
