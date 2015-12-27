package benchmark;

import com.ajjpj.concurrent.pool.AThreadPool;
import com.ajjpj.concurrent.pool.a.AFutureOld;
import com.ajjpj.concurrent.pool.a.APoolOld;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
class NewPoolAdapter_B implements APoolOld {
    final AThreadPool inner;

    public NewPoolAdapter_B (AThreadPool inner) {
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
