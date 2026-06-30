package io.bazel.rulesscala.worker;

import com.google.devtools.build.lib.worker.WorkerProtocol;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A base for JVM workers.
 *
 * <p>This supports regular workers as well as persisent workers. It does not (yet) support
 * multiplexed workers.
 *
 * <p>Worker implementations should implement the `Worker.Interface` interface and provide a main
 * method that calls `Worker.workerMain`.
 */
public final class Worker {

  static final PrintStream BENCH_LOG = openBenchLog();

  private static PrintStream openBenchLog() {
    try {
      return new PrintStream(
          new java.io.FileOutputStream("/tmp/pw-bench.log", true),
          /* autoFlush= */ true);
    } catch (IOException e) {
      return System.err;
    }
  }

  public static interface Interface {
    public void work(String[] args) throws Exception;


    public abstract class WorkerException extends RuntimeException {
      public WorkerException(String message) {
        super(message);
      }
      public WorkerException(String message, Throwable cause) {
        super(message, cause);
      }
    }
  }

  /**
   * The entry point for all workers.
   *
   * <p>This should be the only thing called by a main method in a worker process.
   */
  public static void workerMain(String workerArgs[], Interface workerInterface) throws Exception {
    if (workerArgs.length > 0 && workerArgs[0].equals("--persistent_worker")) {
      persistentWorkerMain(workerInterface);
    } else {
      ephemeralWorkerMain(workerArgs, workerInterface);
    }
  }

  /** The main loop for persistent worker processes */
  private static void persistentWorkerMain(Interface workerInterface) {
    InputStream stdin = System.in;
    PrintStream stdout = System.out;
    PrintStream stderr = System.err;
    ByteArrayOutputStream outStream = new SmartByteArrayOutputStream();
    PrintStream out = new PrintStream(outStream);

    // We can't support stdin, so assign it to read from an empty buffer
    System.setIn(new ByteArrayInputStream(new byte[0]));

    System.setOut(out);
    System.setErr(out);

    try {
      while (true) {
        try {
          WorkerProtocol.WorkRequest request = WorkerProtocol.WorkRequest.parseDelimitedFrom(stdin);

          // The request will be null if stdin is closed.  We're
          // not sure if this happens in TheRealWorld™ but it is
          // useful for testing (to shut down a persistent
          // worker process).
          if (request == null) {
            break;
          }

          int code = 0;
          String[] workerArgs = stringListToArray(request.getArgumentsList());
          BenchSampler.cooldown();
          BenchSampler sampler = BenchSampler.start();

          try {
            String[] args = expandArgsIfArgsfile(workerArgs);
            workerInterface.work(args);
          } catch (Exception e) {
            if (e instanceof Interface.WorkerException) System.err.println(e.getMessage());
            else e.printStackTrace();
            code = 1;
          } finally {
            sampler.stopAndEmit(workerArgs, workerInterface.getClass().getSimpleName());
          }

          WorkerProtocol.WorkResponse.newBuilder()
              .setExitCode(code)
              .setOutput(outStream.toString())
              .build()
              .writeDelimitedTo(stdout);

        } catch (IOException e) {
          // for now we swallow IOExceptions when
          // reading/writing proto
        } finally {
          out.flush();
          outStream.reset();
          System.gc();
        }
      }
    } finally {
      System.setIn(stdin);
      System.setOut(stdout);
      System.setErr(stderr);
    }
  }

  /** The single pass runner for ephemeral (non-persistent) worker processes */
  private static void ephemeralWorkerMain(String workerArgs[], Interface workerInterface)
      throws Exception {
    BenchSampler.cooldown();
    BenchSampler sampler = BenchSampler.start();
    try {
      String[] args = expandArgsIfArgsfile(workerArgs);
      workerInterface.work(args);
    } finally {
      sampler.stopAndEmit(workerArgs, workerInterface.getClass().getSimpleName());
    }
  }

  private static String[] expandArgsIfArgsfile(String[] allArgs) throws IOException {
    if (allArgs.length == 1 && allArgs[0].startsWith("@")) {
        return stringListToArray(
                Files.readAllLines(
                  Paths.get(allArgs[0].substring(1)),
                  StandardCharsets.UTF_8)
                );
      
    } else {
      return allArgs;
    }
  }

  /**
   * A ByteArrayOutputStream that sometimes shrinks its internal buffer during calls to `reset`.
   *
   * <p>In contrast, a regular ByteArrayOutputStream will only ever grow its internal buffer.
   *
   * <p>For an example of subclassing a ByteArrayOutputStream, see Spring's
   * ResizableByteArrayOutputStream:
   * https://github.com/spring-projects/spring-framework/blob/master/spring-core/src/main/java/org/springframework/util/ResizableByteArrayOutputStream.java
   */
  static class SmartByteArrayOutputStream extends ByteArrayOutputStream {
    // ByteArrayOutputStream's defualt Size is 32, which is extremely small
    // to capture stdout from any worker process. We choose a larger default.
    private static final int DEFAULT_SIZE = 256;

    public SmartByteArrayOutputStream() {
      super(DEFAULT_SIZE);
    }

    public boolean isOversized() {
      return this.buf.length > DEFAULT_SIZE;
    }

    @Override
    public void reset() {
      super.reset();
      // reallocate our internal buffer if we've gone over our
      // desired idle size
      if (this.isOversized()) {
        this.buf = new byte[DEFAULT_SIZE];
      }
    }
  }

  private static String[] stringListToArray(List<String> argList) {
    int numArgs = argList.size();
    String[] args = new String[numArgs];
    for (int i = 0; i < numArgs; i++) {
      args[i] = argList.get(i);
    }
    return args;
  }

  /**
   * Host-CPU sampler measuring the duration of one WorkRequest via 1s /proc/stat
   * samples, summarised as peak / avg / peak_10s_window CPU%. Emits one TSV row
   * per request to /tmp/pw-bench.tsv and a human-readable log to /tmp/pw-bench.log.
   * Intended for a serialised measurement run (--jobs=1 --worker_max_instances=1)
   * to size per-target cpu_count exec_properties for remote execution.
   */
  static final class BenchSampler {
    private static final String OUT_PATH = "/tmp/pw-bench.tsv";
    private static final int NCPU = Runtime.getRuntime().availableProcessors();
    private static final Object FILE_LOCK = new Object();

    private final long wallStartNanos;
    private final Thread thread;
    private final List<Double> samples = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean stop = false;

    static BenchSampler start() {
      return new BenchSampler();
    }

    /**
     * Wait for host to settle — instantaneous cpu &lt; 100% (one core, relative)
     * for 2 consecutive seconds. The threshold is just above ambient bazel-server
     * activity between actions; the compile being measured peaks at multiples of
     * this, so the signal still dominates the sample.
     */
    static void cooldown() {
      int requiredCalm = 2;
      int cpuThresholdPct = 100;
      int calm = 0;
      BENCH_LOG.println("[pw-bench] cooldown: waiting for cpu<100% for 2s...");
      try {
        long[] prev = readStat();
        while (calm < requiredCalm) {
          Thread.sleep(1000);
          long[] cur = readStat();
          long db = cur[0] - prev[0];
          long dt = cur[1] - prev[1];
          prev = cur;
          double pct = dt > 0 ? (100.0 * db / dt * NCPU) : 0;
          if (pct <= cpuThresholdPct) {
            calm++;
            BENCH_LOG.printf("[pw-bench] cooldown: cpu=%.0f%% (calm %d/%d)%n", pct, calm, requiredCalm);
          } else {
            if (calm > 0) {
              BENCH_LOG.printf("[pw-bench] cooldown: cpu=%.0f%% (reset, was %d/%d)%n", pct, calm, requiredCalm);
            } else {
              BENCH_LOG.printf("[pw-bench] cooldown: cpu=%.0f%% (waiting...)%n", pct);
            }
            calm = 0;
          }
        }
        BENCH_LOG.println("[pw-bench] cooldown: done");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (IOException ignored) {
      }
    }

    /** Best-effort label extraction across worker arg conventions. */
    static String identifyTarget(String[] args) {
      if (args == null || args.length == 0) return "?";
      for (int i = 0; i < args.length - 1; i++) {
        if ("--CurrentTarget".equals(args[i])) return args[i + 1];
      }
      return args[0];
    }

    private BenchSampler() {
      this.wallStartNanos = System.nanoTime();
      this.thread = new Thread(this::run, "pw-cpu-sampler");
      this.thread.setDaemon(true);
      this.thread.start();
    }

    private static long[] readStat() throws IOException {
      String line;
      try (BufferedReader br = new BufferedReader(new FileReader("/proc/stat"))) {
        line = br.readLine();
      }
      if (line == null) return new long[]{0, 0};
      String[] f = line.trim().split("\\s+");
      long total = 0;
      for (int i = 1; i < f.length; i++) total += Long.parseLong(f[i]);
      long idle = (f.length > 4 ? Long.parseLong(f[4]) : 0)
                + (f.length > 5 ? Long.parseLong(f[5]) : 0);
      return new long[]{total - idle, total};
    }

    private void run() {
      try {
        long[] prev = readStat();
        while (!stop) {
          Thread.sleep(1000);
          long[] cur = readStat();
          long db = cur[0] - prev[0];
          long dt = cur[1] - prev[1];
          double pct = dt > 0 ? (100.0 * db / dt * NCPU) : 0;
          samples.add(pct);
          prev = cur;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (IOException ignored) {
      }
    }

    void stopAndEmit(String[] workerArgs, String mnemonic) {
      stop = true;
      try { thread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      double wallS = (System.nanoTime() - wallStartNanos) / 1e9;
      double peak = 0, sum = 0, peakWin = 0, winSum = 0;
      int n = samples.size();
      int W = 10;
      for (int i = 0; i < n; i++) {
        double v = samples.get(i);
        if (v > peak) peak = v;
        sum += v;
        winSum += v;
        if (i >= W) winSum -= samples.get(i - W);
        if (i >= W - 1) {
          double winAvg = winSum / W;
          if (winAvg > peakWin) peakWin = winAvg;
        }
      }
      double avg = n > 0 ? sum / n : 0;
      if (n > 0 && n < W) peakWin = avg;
      String id = identifyTarget(workerArgs);
      String line = String.format(
          "%s\t%s\t%.2f\t%.1f\t%.1f\t%.1f%n",
          mnemonic, id, wallS, peak, avg, peakWin);
      synchronized (FILE_LOCK) {
        try {
          Files.write(Paths.get(OUT_PATH), line.getBytes(StandardCharsets.UTF_8),
              StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {
        }
      }
      BENCH_LOG.printf("[pw-bench] %s %s wall=%.2fs peak=%.0f%% avg=%.0f%% peak10s=%.0f%%%n",
          mnemonic, id, wallS, peak, avg, peakWin);
    }
  }
}
