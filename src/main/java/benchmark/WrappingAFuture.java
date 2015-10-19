package benchmark;

import com.ajjpj.concurrent.pool.a.AFutureOld;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * @author arno
 */
class WrappingAFuture<T> implements AFutureOld<T> {
    private final Future<T> inner;

    public WrappingAFuture (Future<T> inner) {
        this.inner = inner;
    }

    @Override public boolean isDone () {
        return inner.isDone ();
    }

    @Override public T get () throws InterruptedException, ExecutionException {
        return inner.get ();
    }
}
