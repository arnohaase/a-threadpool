package benchmark;

import java.util.concurrent.ExecutionException;


/**
 * @author arno
 */
public interface ABenchmarkFuture<T> {
    boolean isDone ();
    T get () throws InterruptedException, ExecutionException;
}
