package benchmark;

import com.ajjpj.concurrent.pool.a.AFutureOld;
import com.ajjpj.concurrent.pool.a.APoolOld;
import com.ajjpj.concurrent.pool.b.AThreadPoolImpl;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
class NewPoolAdapter_B implements APoolOld {
    final AThreadPoolImpl inner;

    public NewPoolAdapter_B (AThreadPoolImpl inner) {
        this.inner = inner;
    }

    @Override public <T> AFutureOld<T> submit (Callable<T> code) {
        return new WrappingAFuture<> (inner.submit (code));
    }

    @Override public void shutdown () throws InterruptedException {
        inner.shutdown ();
        inner.awaitTermination ();
    }
}
