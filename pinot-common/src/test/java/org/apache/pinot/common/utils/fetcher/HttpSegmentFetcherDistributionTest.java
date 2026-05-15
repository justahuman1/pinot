/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils.fetcher;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.RoundRobinURIProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Investigative test: measures the distribution of segment download requests across K mock "controller" endpoints
 * when a server fetches N segments via the RoundRobinURIProvider + HttpSegmentFetcher path.
 *
 * <p>Background: A code-only investigation claimed two imbalance mechanisms:
 * <ol>
 *   <li>RoundRobinURIProvider picks a random start per call, and HttpSegmentFetcher exits on first success,
 *       so effectively one IP per segment download.</li>
 *   <li>Apache HttpClient5 PoolingHttpClientConnectionManager reuses connections by (scheme, host, port),
 *       claimed to "amplify" the imbalance.</li>
 * </ol>
 *
 * <p>This test exercises the real production code path to measure whether the distribution is actually
 * non-uniform enough to explain ~half the controllers being idle.
 *
 * <p>NOTE on DNS resolution limitation: {@code HttpSegmentFetcher#fetchSegmentToLocal(URI, File)} calls
 * {@code new RoundRobinURIProvider(List.of(downloadURI), resolveHost=true)}, which expands a single
 * hostname URI into one URI per resolved IP. Since we cannot control DNS in a unit test environment,
 * this test instead directly exercises the RoundRobinURIProvider random-start selection mechanism:
 * for each simulated segment download, a fresh RoundRobinURIProvider is constructed (mirroring the
 * per-call instantiation in HttpSegmentFetcher) and {@code next()} is called once (mirroring the
 * first-success-stops behavior). The chosen URI is then fetched via a real HTTP call to a mock server.
 *
 * <p>The distribution result tells us whether the random-start + first-success pattern is uniform
 * or skewed across K controllers.
 */
public class HttpSegmentFetcherDistributionTest {

  private static final int NUM_CONTROLLERS = 10;
  private static final int NUM_DOWNLOADS = 1000;
  private static final int BASE_PORT = 20000;
  private static final String SEGMENT_CONTENT = "mock-segment-payload";

  private final List<HttpServer> _servers = new ArrayList<>();
  private final ConcurrentHashMap<Integer, AtomicInteger> _requestCounts = new ConcurrentHashMap<>();
  // For delay variant: one server (index 0) will add artificial delay
  private final ConcurrentHashMap<Integer, AtomicInteger> _delayRequestCounts = new ConcurrentHashMap<>();
  private static final int DELAY_SERVER_INDEX = 0;
  private static final int DELAY_MS = 200;

  private List<URI> _serverUris;
  private HttpSegmentFetcher _fetcher;
  private File _tempDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = FileUtils.getTempDirectory();

    // Initialize request counters
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      _requestCounts.put(i, new AtomicInteger(0));
      _delayRequestCounts.put(i, new AtomicInteger(0));
    }

    // Spin up K mock HTTP servers, each recording every incoming request
    _serverUris = new ArrayList<>();
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      int port = BASE_PORT + i;
      final int serverIndex = i;
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 50);
      server.createContext("/segments/download", new CountingHandler(serverIndex, _requestCounts, 0));
      server.setExecutor(null);
      server.start();
      _servers.add(server);
      _serverUris.add(new URI("http://127.0.0.1:" + port + "/segments/download"));
    }

    // Initialize HttpSegmentFetcher with real config (short timeouts for test speed)
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
    config.setProperty(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 10);
    config.setProperty(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 1.1);
    config.setProperty(HttpSegmentFetcher.CONNECTION_REQUEST_TIMEOUT_CONFIG_KEY, 5000);
    config.setProperty(HttpSegmentFetcher.SOCKET_TIMEOUT_CONFIG_KEY, 5000);

    _fetcher = new HttpSegmentFetcher();
    _fetcher.init(config);
  }

  @AfterClass
  public void tearDown() {
    _servers.forEach(s -> s.stop(0));
  }

  /**
   * Variant 1 (no delay): Measures RoundRobinURIProvider distribution under ideal conditions.
   *
   * <p>Each of N downloads creates a fresh RoundRobinURIProvider over K URIs (mirroring HttpSegmentFetcher's
   * per-call instantiation), picks the first URI via {@code next()}, and makes a real HTTP GET to that server.
   * Since all servers respond instantly, this isolates the effect of the random-start selection.
   *
   * <p>Expected if hypothesis is WRONG (distribution is uniform): chi-squared p-value > 0.05,
   * imbalance ratio < 2x.
   * <p>Expected if hypothesis is RIGHT (distribution is skewed): imbalance ratio > 5x, or some IPs
   * get near-zero requests.
   */
  @Test
  public void testDistributionNoDelay()
      throws Exception {
    System.out.println("\n========== VARIANT 1: No-Delay Distribution ==========");
    System.out.printf("Simulating %d segment downloads across %d controllers (no artificial delay)%n%n",
        NUM_DOWNLOADS, NUM_CONTROLLERS);

    for (int n = 0; n < NUM_DOWNLOADS; n++) {
      // Simulate HttpSegmentFetcher: create a fresh RoundRobinURIProvider per download call.
      // resolveHost=false because URIs are already IP:port — mirrors the in-loop behavior for the case
      // where the caller has already resolved hosts into a list, or when we bypass DNS.
      RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(_serverUris, false);
      URI chosenUri = uriProvider.next(); // first pick = what HttpSegmentFetcher uses on first (successful) attempt

      // Real HTTP call via actual HttpSegmentFetcher path
      File dest = File.createTempFile("seg-" + n + "-", ".bin", _tempDir);
      dest.deleteOnExit();
      _fetcher.fetchSegmentToLocalWithoutRetry(chosenUri, dest);
    }

    int[] counts = new int[NUM_CONTROLLERS];
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      counts[i] = _requestCounts.get(i).get();
    }
    printDistributionReport("No-Delay", counts, NUM_DOWNLOADS);
  }

  /**
   * Variant 2 (one slow server): Tests whether connection-pool warmth/stickiness biases distribution.
   *
   * <p>One server (index 0) adds a 200ms artificial delay to its responses. If the connection pool
   * "amplifies" imbalance (the claimed Mechanism #2), the slow server should receive a noticeably
   * different share of requests compared to the no-delay variant — because slow connections take
   * longer to return to the pool, potentially making other IPs "warmer" in the pool.
   *
   * <p>Note: IP selection via RoundRobinURIProvider happens BEFORE the connection pool is consulted,
   * so pool stickiness should NOT change which IP is selected. If the two distributions are similar,
   * Mechanism #2 is refuted.
   */
  @Test(dependsOnMethods = "testDistributionNoDelay")
  public void testDistributionWithOneSlowServer()
      throws Exception {
    System.out.println("\n========== VARIANT 2: One-Slow-Server Distribution ==========");
    System.out.printf("Server %d (port %d) will delay %dms per response%n%n",
        DELAY_SERVER_INDEX, BASE_PORT + DELAY_SERVER_INDEX, DELAY_MS);

    // Replace the handler for server 0 with a slow one
    HttpServer slowServer = _servers.get(DELAY_SERVER_INDEX);
    slowServer.removeContext("/segments/download");
    slowServer.createContext("/segments/download",
        new CountingHandler(DELAY_SERVER_INDEX, _delayRequestCounts, DELAY_MS));

    // Replace all other servers with fresh handlers pointing to _delayRequestCounts
    for (int i = 1; i < NUM_CONTROLLERS; i++) {
      HttpServer s = _servers.get(i);
      s.removeContext("/segments/download");
      final int idx = i;
      s.createContext("/segments/download", new CountingHandler(idx, _delayRequestCounts, 0));
    }

    for (int n = 0; n < NUM_DOWNLOADS; n++) {
      RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(_serverUris, false);
      URI chosenUri = uriProvider.next();

      File dest = File.createTempFile("seg-delay-" + n + "-", ".bin", _tempDir);
      dest.deleteOnExit();
      _fetcher.fetchSegmentToLocalWithoutRetry(chosenUri, dest);
    }

    int[] counts = new int[NUM_CONTROLLERS];
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      counts[i] = _delayRequestCounts.get(i).get();
    }
    printDistributionReport("One-Slow-Server (server 0 = " + DELAY_MS + "ms delay)", counts, NUM_DOWNLOADS);

    System.out.println("\n>>> Compare Variant 1 vs Variant 2 per-server counts:");
    System.out.printf("  %-8s %-15s %-15s %-12s%n", "Server", "NoDelay Count", "SlowDelay Count", "Delta");
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      int noDelay = _requestCounts.get(i).get();
      int withDelay = counts[i];
      System.out.printf("  %-8d %-15d %-15d %-12d%s%n", i, noDelay, withDelay, (withDelay - noDelay),
          i == DELAY_SERVER_INDEX ? "  <-- SLOW SERVER" : "");
    }
    System.out.println();
  }

  // -------------------------------------------------------------------------
  // Helper: statistics and report
  // -------------------------------------------------------------------------

  private static void printDistributionReport(String label, int[] counts, int totalDownloads) {
    double expected = (double) totalDownloads / counts.length;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    long sum = 0;
    for (int c : counts) {
      min = Math.min(min, c);
      max = Math.max(max, c);
      sum += c;
    }
    double mean = (double) sum / counts.length;
    double variance = 0;
    for (int c : counts) {
      double d = c - mean;
      variance += d * d;
    }
    double stddev = Math.sqrt(variance / counts.length);

    // Chi-squared statistic: sum((observed - expected)^2 / expected)
    double chiSquared = 0;
    for (int c : counts) {
      double diff = c - expected;
      chiSquared += (diff * diff) / expected;
    }
    double imbalanceRatio = (min == 0) ? Double.POSITIVE_INFINITY : (double) max / min;

    System.out.printf("[%s] Distribution across %d servers (%d downloads):%n", label, counts.length, totalDownloads);
    System.out.printf("  Per-server request counts:%n");
    for (int i = 0; i < counts.length; i++) {
      double pct = 100.0 * counts[i] / totalDownloads;
      System.out.printf("    Server %2d (port %d): %4d  (%.1f%%)%n", i, BASE_PORT + i, counts[i], pct);
    }
    System.out.printf("  Expected per server:   %.1f%n", expected);
    System.out.printf("  Min: %d  Max: %d  Mean: %.1f  StdDev: %.1f%n", min, max, mean, stddev);
    System.out.printf("  Chi-squared statistic: %.2f (df=%d)%n", chiSquared, counts.length - 1);
    System.out.printf("  Imbalance ratio (max/min): %.2fx%n", imbalanceRatio);

    // Interpretation
    // chi-squared critical value at p=0.05, df=9 is ~16.92
    double chiSquaredCritical = 16.92;
    boolean chiOk = chiSquared < chiSquaredCritical;
    boolean imbalanceOk = imbalanceRatio < 2.0;

    if (chiOk && imbalanceOk) {
      System.out.printf("%n  *** EVIDENCE AGAINST IMBALANCE [%s]: distribution is statistically uniform.%n", label);
      System.out.printf("      chi2=%.2f < %.2f (p>0.05), imbalance ratio=%.2fx < 2x%n",
          chiSquared, chiSquaredCritical, imbalanceRatio);
      System.out.printf("      The random-start selection pattern does NOT produce significant load imbalance.%n");
    } else {
      System.out.printf("%n  *** EVIDENCE FOR IMBALANCE [%s]: distribution is SKEWED.%n", label);
      System.out.printf("      chi2=%.2f vs critical=%.2f, imbalance ratio=%.2fx%n",
          chiSquared, chiSquaredCritical, imbalanceRatio);
      System.out.printf("      The random-start selection pattern DOES produce significant load imbalance.%n");
    }
    System.out.println();
  }

  // -------------------------------------------------------------------------
  // Mock HTTP handler: records each request and returns minimal 200 payload
  // -------------------------------------------------------------------------

  private static class CountingHandler implements HttpHandler {
    private final int _serverIndex;
    private final ConcurrentHashMap<Integer, AtomicInteger> _counts;
    private final int _delayMs;

    CountingHandler(int serverIndex, ConcurrentHashMap<Integer, AtomicInteger> counts, int delayMs) {
      _serverIndex = serverIndex;
      _counts = counts;
      _delayMs = delayMs;
    }

    @Override
    public void handle(HttpExchange exchange)
        throws IOException {
      _counts.get(_serverIndex).incrementAndGet();

      if (_delayMs > 0) {
        try {
          Thread.sleep(_delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      byte[] body = SEGMENT_CONTENT.getBytes();
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }
}
