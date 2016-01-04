package benchmark;

import com.ajjpj.concurrent.pool.ASharedQueueStatistics;
import com.ajjpj.concurrent.pool.AThreadPoolStatistics;
import com.ajjpj.concurrent.pool.AWorkerThreadStatistics;
import com.ajjpj.concurrent.pool.a.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.*;


/**
 * @author arno
 */
//@Fork (2)
@Fork (0)
//@Fork (1)
@Threads (1)
@Warmup (iterations = 3, time = 1)
@Measurement (iterations = 3, time = 3)
@State (Scope.Benchmark)
public class PoolBenchmark {
    APoolOld pool;

    @Param ({
            "b-only-scanner-clears",
//            "b-steal-only-stable",
//            "b-scan-till-quiet",
//            "b-scanning-counter",
            "b-scanning-flag",
//            "naive",
//            "a-global-queue",
//            "work-stealing",
//            "a-strict-own",
//            "Fixed",
//            "ForkJoinSharedQueues",
//            "ForkJoinLifo",
//            "ForkJoinFifo",
//            "J9FjSharedQueues",
            "J9FjLifo",
//            "J9FjFifo"
    })
    public String strategy;

    @Setup
    public void setUp() {
        switch (strategy) {
            case "b-scanning-flag":       pool = new NewPoolAdapter_B (new com.ajjpj.concurrent.pool._01_scanning_flag.AThreadPoolImpl(8, 16384, 16384)); break;
            case "b-scanning-counter":    pool = new NewPoolAdapter_B (new com.ajjpj.concurrent.pool._02_scanningcounter.AThreadPoolImpl(8, 16384, 16384)); break;
            case "b-scan-till-quiet":     pool = new NewPoolAdapter_B (new com.ajjpj.concurrent.pool._03_scan_till_quiet.AThreadPoolImpl(8, 16384, 16384)); break;
            case "b-steal-only-stable":   pool = new NewPoolAdapter_B (new com.ajjpj.concurrent.pool._04_steal_only_stable.AThreadPoolImpl(8, 16384, 16384)); break;
            case "b-only-scanner-clears": pool = new NewPoolAdapter_B (new com.ajjpj.concurrent.pool._05_only_scanning_thread_clears_flag.AThreadPoolImpl(8, 16384, 16384)); break;
            case "naive":          pool = new NaivePool (8); break;
            case "a-global-queue": pool = new APoolImpl (8, ASchedulingStrategy.SingleQueue ()).start (); break;
            case "a-strict-own":   pool = new APoolImpl (32, ASchedulingStrategy.OWN_FIRST_NO_STEALING).start (); break;
//            case "work-stealing":  pool = new WorkStealingPoolImpl (1).start (); break;
            case "work-stealing":  pool = new WorkStealingPoolImpl (8).start (); break;
            case "Fixed":          pool = new DelegatingPool (Executors.newFixedThreadPool (8)); break;

            case "ForkJoinSharedQueues": pool = new DelegatingPool (ForkJoinPool.commonPool ()); break;
            case "ForkJoinLifo":         pool = new ForkJoinForkingPool (createForkJoin (false)); break;
            case "ForkJoinFifo":         pool = new ForkJoinForkingPool (createForkJoin (true)); break;

            case "J9FjSharedQueues": pool = new DelegatingPool (createJ9ForkJoin (8, false)); break;
            case "J9FjLifo":         pool = new J9NewForkingPool (createJ9ForkJoin (8, false)); break;
            case "J9FjFifo":         pool = new J9NewForkingPool (createJ9ForkJoin (8, true)); break;

            default: throw new IllegalStateException ();
        }
    }

    private static ForkJoinPool createForkJoin (boolean fifo) {
        final ForkJoinPool p = ForkJoinPool.commonPool ();
        return new ForkJoinPool (p.getParallelism (), p.getFactory (), p.getUncaughtExceptionHandler (), fifo);
    }

    private static jdk.j9new.ForkJoinPool createJ9ForkJoin (int numThreads, boolean fifo) {
        return new jdk.j9new.ForkJoinPool (numThreads, new J9LimitingForkJoinThreadFactory(numThreads), null, fifo);
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        pool.shutdown ();

        if (pool instanceof NewPoolAdapter_B) {
            System.out.println ();
            System.out.println ("---- Thread Pool Statistics ----");

            final AThreadPoolStatistics stats = ((NewPoolAdapter_B) pool).inner.getStatistics ();
            for (ASharedQueueStatistics s: stats.sharedQueueStatisticses) {
                System.out.println (s);
            }
            for (AWorkerThreadStatistics s: stats.workerThreadStatistics) {
                System.out.println (s);
            }
            System.out.println ("--------------------------------");
        }
    }

    @Benchmark
    public void _testSimpleScheduling01() throws InterruptedException {
        final int num = 10_000;
        final CountDownLatch latch = new CountDownLatch (num);

        for (int i=0; i<num; i++) {
            pool.submit (() -> {
                latch.countDown ();
                return null;});
        }
//        System.err.println ("### finished submitting");
        latch.await ();
//        System.err.println ("---");
    }

    @Benchmark
    @Threads (7)
    public void _testSimpleScheduling07() throws InterruptedException {
        final int num = 10_000;
        final CountDownLatch latch = new CountDownLatch (num);

        for (int i=0; i<num; i++) {
            pool.submit (() -> {
                latch.countDown ();
                return null;});
        }
//        System.err.println ("### finished submitting");
        latch.await ();
//        System.err.println ("---");
    }

    @Benchmark
    public void testFactorialSingle() throws ExecutionException, InterruptedException {
        final SettableFutureTask<Long> fact = new SettableFutureTask<> (() -> null);
        fact (1, 12, fact);
        fact.get ();
    }

    @Benchmark
    @Threads (7)
    public void testFactorialMulti7() throws ExecutionException, InterruptedException {
        final SettableFutureTask<Long> fact = new SettableFutureTask<> (() -> null);
        fact (1, 12, fact);
        fact.get ();
    }

    @Benchmark
    @Threads (8)
    public void testFactorialMulti8() throws ExecutionException, InterruptedException {
        final SettableFutureTask<Long> fact = new SettableFutureTask<> (() -> null);
        fact (1, 12, fact);
        fact.get ();
    }

    void fact (long collect, int n, SettableFutureTask<Long> result) {
        if (n <= 1) {
            result.set (collect);
        }
        else {
            pool.submit (() -> {
                fact (collect * n, n-1, result);
                return null;
            });
        }
    }

    public static class SettableFutureTask<T> extends FutureTask<T> {
        public SettableFutureTask (Callable<T> callable) {
            super (callable);
        }

        @Override public void set (T t) {
            super.set (t);
        }

        @Override public void setException (Throwable t) {
            super.setException (t);
        }
    }

//    @Benchmark
    public void testRecursiveFibo() throws ExecutionException, InterruptedException {
        fibo (8);
    }

    long fibo (long n) throws ExecutionException, InterruptedException {
        if (n <= 1) return 1;

        final AFutureOld<Long> f = pool.submit (() -> fibo(n-1));
        return f.get() + pool.submit (() -> fibo(n-2)).get ();
    }

    @Benchmark
    public void _testStealVeryCheap() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch (10_000);

        pool.submit (() -> {
           for (int i=0; i<10_000; i++) {
               pool.submit (() -> {
                   latch.countDown();
                   return null;
               });
           }
            return null;
        });
        latch.await ();
    }

    @Benchmark
    public void __testStealExpensive() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch (10_000);

        pool.submit (() -> {
           for (int i=0; i<10_000; i++) {
               pool.submit (() -> {
                   Blackhole.consumeCPU (100);
                   latch.countDown();
                   return null;
               });
           }
            return null;
        });
        latch.await ();
//        System.out.println ("--");
    }

    @Benchmark
    public void testPingPong01() throws InterruptedException {
        testPingPong (1);
    }

    @Benchmark
    public void testPingPong02() throws InterruptedException {
        testPingPong (2);
    }

    @Benchmark
    public void testPingPong07() throws InterruptedException {
        testPingPong (7);
    }

    @Benchmark
    public void testPingPong32() throws InterruptedException {
        testPingPong (32);
    }

    private void testPingPong(int numThreads) throws InterruptedException {
        final PingPongActor a1 = new PingPongActor ();
        final PingPongActor a2 = new PingPongActor ();

        final CountDownLatch latch = new CountDownLatch (numThreads);

        for (int i=0; i<numThreads; i++) {
            a1.receive (a2, 10_000, latch::countDown);
        }

        latch.await ();
    }

    class PingPongActor {
        void receive (PingPongActor sender, int remaining, Runnable onFinished) {
            if (remaining == 0) {
                onFinished.run ();
            }
            else {
                pool.submit (() -> {sender.receive (PingPongActor.this, remaining-1, onFinished); return null;});
            }
        }
    }
}

/*

Benchmark                                                                  (strategy)   Mode  Samples        Score  Score error  Units
benchmark.PoolBenchmark._testSimpleScheduling                                   naive  thrpt        3      509,824       24,143  ops/s
benchmark.PoolBenchmark._testSimpleScheduling                                   Fixed  thrpt        3      385,665       20,206  ops/s
benchmark.PoolBenchmark._testSimpleScheduling                    ForkJoinSharedQueues  thrpt        3      540,563       38,241  ops/s
benchmark.PoolBenchmark._testSimpleScheduling                            ForkJoinLifo  thrpt        3      541,560       30,508  ops/s
benchmark.PoolBenchmark._testSimpleScheduling                            ForkJoinFifo  thrpt        3      536,702       11,905  ops/s

benchmark.PoolBenchmark._testStealExpensive                                     naive  thrpt        3      213,353        6,779  ops/s
benchmark.PoolBenchmark._testStealExpensive                                     Fixed  thrpt        3      204,565        4,240  ops/s
benchmark.PoolBenchmark._testStealExpensive                      ForkJoinSharedQueues  thrpt        3      589,620       12,907  ops/s
benchmark.PoolBenchmark._testStealExpensive                              ForkJoinLifo  thrpt        3      792,421       29,601  ops/s
benchmark.PoolBenchmark._testStealExpensive                              ForkJoinFifo  thrpt        3      687,161        9,575  ops/s

benchmark.PoolBenchmark._testStealVeryCheap                                     naive  thrpt        3      469,721        3,749  ops/s
benchmark.PoolBenchmark._testStealVeryCheap                                     Fixed  thrpt        3      370,198       21,857  ops/s
benchmark.PoolBenchmark._testStealVeryCheap                      ForkJoinSharedQueues  thrpt        3      535,963       27,097  ops/s
benchmark.PoolBenchmark._testStealVeryCheap                              ForkJoinLifo  thrpt        3      745,794       34,951  ops/s
benchmark.PoolBenchmark._testStealVeryCheap                              ForkJoinFifo  thrpt        3      597,199       17,109  ops/s

benchmark.PoolBenchmark.testFactorialMulti7                                     naive  thrpt        3   316508,717    28632,128  ops/s
benchmark.PoolBenchmark.testFactorialMulti7                                     Fixed  thrpt        3   295738,885    14755,405  ops/s
benchmark.PoolBenchmark.testFactorialMulti7                      ForkJoinSharedQueues  thrpt        3   632280,083    36331,379  ops/s
benchmark.PoolBenchmark.testFactorialMulti7                              ForkJoinLifo  thrpt        3   828421,444    42517,529  ops/s
benchmark.PoolBenchmark.testFactorialMulti7                              ForkJoinFifo  thrpt        3   948692,816    33348,797  ops/s

benchmark.PoolBenchmark.testFactorialMulti8                                     naive  thrpt        3   320309,918    12719,970  ops/s
benchmark.PoolBenchmark.testFactorialMulti8                                     Fixed  thrpt        3   292813,644    16496,237  ops/s
benchmark.PoolBenchmark.testFactorialMulti8                      ForkJoinSharedQueues  thrpt        3   644153,505   165698,109  ops/s
benchmark.PoolBenchmark.testFactorialMulti8                              ForkJoinLifo  thrpt        3  1019960,167    87225,783  ops/s
benchmark.PoolBenchmark.testFactorialMulti8                              ForkJoinFifo  thrpt        3  1017455,379    56650,324  ops/s

benchmark.PoolBenchmark.testFactorialSingle                                     naive  thrpt        3    96637,398    13705,504  ops/s
benchmark.PoolBenchmark.testFactorialSingle                                     Fixed  thrpt        3    88768,903    14078,287  ops/s
benchmark.PoolBenchmark.testFactorialSingle                      ForkJoinSharedQueues  thrpt        3    65945,768     1303,415  ops/s
benchmark.PoolBenchmark.testFactorialSingle                              ForkJoinLifo  thrpt        3    74300,901      968,355  ops/s
benchmark.PoolBenchmark.testFactorialSingle                              ForkJoinFifo  thrpt        3    73282,622      642,859  ops/s

benchmark.PoolBenchmark.testPingPong1                                           naive  thrpt        3      363,229        7,503  ops/s
benchmark.PoolBenchmark.testPingPong1                                           Fixed  thrpt        3      312,936        7,022  ops/s
benchmark.PoolBenchmark.testPingPong1                            ForkJoinSharedQueues  thrpt        3      282,892        4,938  ops/s
benchmark.PoolBenchmark.testPingPong1                                    ForkJoinLifo  thrpt        3      440,813       21,186  ops/s
benchmark.PoolBenchmark.testPingPong1                                    ForkJoinFifo  thrpt        3      515,137      188,272  ops/s

benchmark.PoolBenchmark.testPingPong2                                           naive  thrpt        3      200,233        1,811  ops/s
benchmark.PoolBenchmark.testPingPong2                                           Fixed  thrpt        3      169,749        1,425  ops/s
benchmark.PoolBenchmark.testPingPong2                            ForkJoinSharedQueues  thrpt        3      277,558       13,981  ops/s
benchmark.PoolBenchmark.testPingPong2                                    ForkJoinLifo  thrpt        3      555,811        2,869  ops/s
benchmark.PoolBenchmark.testPingPong2                                    ForkJoinFifo  thrpt        3      779,076       57,664  ops/s

benchmark.PoolBenchmark.testPingPong32                                          naive  thrpt        3       10,978        1,390  ops/s
benchmark.PoolBenchmark.testPingPong32                                          Fixed  thrpt        3        9,961        0,283  ops/s
benchmark.PoolBenchmark.testPingPong32                           ForkJoinSharedQueues  thrpt        3       44,052        5,058  ops/s
benchmark.PoolBenchmark.testPingPong32                                   ForkJoinLifo  thrpt        3      246,309       26,319  ops/s
benchmark.PoolBenchmark.testPingPong32                                   ForkJoinFifo  thrpt        3      244,104       21,847  ops/s

benchmark.PoolBenchmark.testPingPong7                                           naive  thrpt        3       53,677        7,886  ops/s
benchmark.PoolBenchmark.testPingPong7                                           Fixed  thrpt        3       50,216       53,819  ops/s
benchmark.PoolBenchmark.testPingPong7                            ForkJoinSharedQueues  thrpt        3      184,192        9,848  ops/s
benchmark.PoolBenchmark.testPingPong7                                    ForkJoinLifo  thrpt        3     1185,856      272,533  ops/s
benchmark.PoolBenchmark.testPingPong7                                    ForkJoinFifo  thrpt        3      997,910      160,875  ops/s

benchmark.PoolBenchmark.testRecursiveFibo                                       naive  thrpt        3     1954,631       66,994  ops/s
benchmark.PoolBenchmark.testRecursiveFibo                                       Fixed  thrpt        3     1971,530      118,138  ops/s
benchmark.PoolBenchmark.testRecursiveFibo                        ForkJoinSharedQueues  thrpt        3     3693,364     1181,157  ops/s
benchmark.PoolBenchmark.testRecursiveFibo                                ForkJoinLifo  thrpt        3     7173,006       85,762  ops/s
benchmark.PoolBenchmark.testRecursiveFibo                                ForkJoinFifo  thrpt        3     7168,285      226,860  ops/s

---

Benchmark                                          (strategy)   Mode  Samples      Score  Score error  Units
b.PoolBenchmark._testSimpleScheduling    ForkJoinSharedQueues  thrpt        3    544,755       11,597  ops/s
b.PoolBenchmark._testSimpleScheduling            ForkJoinLifo  thrpt        3    541,732       11,450  ops/s
b.PoolBenchmark._testSimpleScheduling            ForkJoinFifo  thrpt        3    538,158       30,292  ops/s
b.PoolBenchmark._testSimpleScheduling        J9FjSharedQueues  thrpt        3    529,955       13,634  ops/s
b.PoolBenchmark._testSimpleScheduling                J9FjLifo  thrpt        3    520,372       61,772  ops/s
b.PoolBenchmark._testSimpleScheduling                J9FjFifo  thrpt        3    526,000       54,109  ops/s

b.PoolBenchmark._testStealExpensive      ForkJoinSharedQueues  thrpt        3    585,362       13,384  ops/s
b.PoolBenchmark._testStealExpensive              ForkJoinLifo  thrpt        3    797,045       13,124  ops/s
b.PoolBenchmark._testStealExpensive              ForkJoinFifo  thrpt        3    680,294       19,275  ops/s
b.PoolBenchmark._testStealExpensive          J9FjSharedQueues  thrpt        3    492,861       71,993  ops/s
b.PoolBenchmark._testStealExpensive                  J9FjLifo  thrpt        3    752,447       27,421  ops/s
b.PoolBenchmark._testStealExpensive                  J9FjFifo  thrpt        3    631,409       43,113  ops/s

b.PoolBenchmark._testStealVeryCheap      ForkJoinSharedQueues  thrpt        3    527,263       43,969  ops/s
b.PoolBenchmark._testStealVeryCheap              ForkJoinLifo  thrpt        3    739,734       30,741  ops/s
b.PoolBenchmark._testStealVeryCheap              ForkJoinFifo  thrpt        3    591,970        7,302  ops/s
b.PoolBenchmark._testStealVeryCheap          J9FjSharedQueues  thrpt        3    493,326       29,714  ops/s
b.PoolBenchmark._testStealVeryCheap                  J9FjLifo  thrpt        3    687,577       34,213  ops/s
b.PoolBenchmark._testStealVeryCheap                  J9FjFifo  thrpt        3    554,378       48,516  ops/s

b.PoolBenchmark.testFactorialSingle      ForkJoinSharedQueues  thrpt        3  66334,205      154,651  ops/s
b.PoolBenchmark.testFactorialSingle              ForkJoinLifo  thrpt        3  72912,783     2096,798  ops/s
b.PoolBenchmark.testFactorialSingle              ForkJoinFifo  thrpt        3  72633,694      741,742  ops/s
b.PoolBenchmark.testFactorialSingle          J9FjSharedQueues  thrpt        3  59567,361      605,596  ops/s
b.PoolBenchmark.testFactorialSingle                  J9FjLifo  thrpt        3  64273,864     1880,293  ops/s
b.PoolBenchmark.testFactorialSingle                  J9FjFifo  thrpt        3  64408,868      559,736  ops/s

b.PoolBenchmark.testPingPong07           ForkJoinSharedQueues  thrpt        3    182,371        8,880  ops/s
b.PoolBenchmark.testPingPong07                   ForkJoinLifo  thrpt        3   1195,404      114,326  ops/s
b.PoolBenchmark.testPingPong07                   ForkJoinFifo  thrpt        3   1118,761       15,011  ops/s
b.PoolBenchmark.testPingPong07               J9FjSharedQueues  thrpt        3    183,397       42,063  ops/s
b.PoolBenchmark.testPingPong07                       J9FjLifo  thrpt        3    862,897       57,953  ops/s
b.PoolBenchmark.testPingPong07                       J9FjFifo  thrpt        3    720,931       98,262  ops/s

b.PoolBenchmark.testPingPong32           ForkJoinSharedQueues  thrpt        3     44,867        5,437  ops/s
b.PoolBenchmark.testPingPong32                   ForkJoinLifo  thrpt        3    250,222       14,559  ops/s
b.PoolBenchmark.testPingPong32                   ForkJoinFifo  thrpt        3    246,045        4,561  ops/s
b.PoolBenchmark.testPingPong32               J9FjSharedQueues  thrpt        3     44,860       11,245  ops/s
b.PoolBenchmark.testPingPong32                       J9FjLifo  thrpt        3    243,504       55,615  ops/s
b.PoolBenchmark.testPingPong32                       J9FjFifo  thrpt        3    178,584        5,017  ops/s

b.PoolBenchmark.testRecursiveFibo        ForkJoinSharedQueues  thrpt        3   3666,498      502,876  ops/s
b.PoolBenchmark.testRecursiveFibo                ForkJoinLifo  thrpt        3   7070,330      268,687  ops/s
b.PoolBenchmark.testRecursiveFibo                ForkJoinFifo  thrpt        3   7155,278     1346,646  ops/s
b.PoolBenchmark.testRecursiveFibo            J9FjSharedQueues  thrpt        3   3102,038      500,588  ops/s
b.PoolBenchmark.testRecursiveFibo                    J9FjLifo  thrpt        3   6363,291      548,086  ops/s
b.PoolBenchmark.testRecursiveFibo                    J9FjFifo  thrpt        3   6399,835      808,548  ops/s




 */
