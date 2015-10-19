package benchmark;

import com.ajjpj.concurrent.pool.a.AFutureOld;
import com.ajjpj.concurrent.pool.a.APoolOld;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;


/**
 * @author arno
 */
public class DelegatingPool implements APoolOld {
    private final ExecutorService ec;

    public DelegatingPool (ExecutorService ec) {
        this.ec = ec;
    }

    @Override public <T> AFutureOld<T> submit (Callable<T> code) {
        return new WrappingAFuture<> (ec.submit (code));
    }

    @Override public void shutdown () throws InterruptedException {
        ec.shutdown ();
    }
}
