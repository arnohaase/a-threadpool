package benchmark;

import com.ajjpj.concurrent.pool.a.AFutureOld;
import com.ajjpj.concurrent.pool.a.APoolOld;
import com.ajjpj.concurrent.pool.api.AThreadPoolStatistics;
import com.ajjpj.concurrent.pool.api.AThreadPoolWithAdmin;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;


public class AThreadPoolAdapter implements APoolOld {
    final AThreadPoolWithAdmin inner;

    public AThreadPoolAdapter (AThreadPoolWithAdmin inner) {
        this.inner = inner;
    }

    @Override public void submit (Runnable code) {
        inner.submit (code);
    }

    @Override public AThreadPoolStatistics getStatistics () {
        return inner.getStatistics ();
    }

    @Override public <T> AFutureOld<T> submit (Callable<T> code) {
        final SettableFutureTask<T> f = new SettableFutureTask<> (code);
        final WrappingAFuture<T> result = new WrappingAFuture<> (f);

        submit (() -> {
            try {
                f.set (code.call ());
            }
            catch (Exception e) {
                e.printStackTrace ();
            }
        });


        return result;
    }

    @Override public void shutdown () throws InterruptedException {
        inner.shutdown ();
    }
}
