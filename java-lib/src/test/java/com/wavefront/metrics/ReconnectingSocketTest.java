package com.wavefront.metrics;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ReconnectingSocketTest {
  protected static final Logger logger = Logger.getLogger(ReconnectingSocketTest.class.getCanonicalName());

  private Thread testServer;
  private ReconnectingSocket toServer;
  private int connects = 0;

  @Before
  public void initTestServer() throws IOException, InterruptedException {
    connects = 0;
    final ServerSocket serverSocket = new ServerSocket(0);

    testServer = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Socket fromClient = serverSocket.accept();
          connects++;
          BufferedReader inFromClient = new BufferedReader(new InputStreamReader(fromClient.getInputStream()));
          while (true) {
            String input = inFromClient.readLine().trim().toLowerCase();
            if (input.equals("give_fin")) {
              fromClient.shutdownOutput();
              break;  // Go to outer while loop, accept a new socket from serverSocket.
            } else if (input.equals("give_rst")) {
              fromClient.setSoLinger(true, 0);
              fromClient.close();
              break;  // Go to outer while loop, accept a new socket from serverSocket.
            }
          }
        } catch (IOException e) {
          logger.log(Level.SEVERE, "Unexpected test error.", e);
          // OK to go back to the top of the loop.
        }
      }
    });

    testServer.start();
    toServer = new ReconnectingSocket("localhost", serverSocket.getLocalPort());
  }

  @After
  public void teardownTestServer() throws IOException {
    testServer.interrupt();
    toServer.close();
  }

  @Test(timeout = 5000L)
  public void testReconnect() throws Exception {
    toServer.write("ping\n");
    toServer.flush();
    toServer.write("give_fin\n");
    toServer.flush();
    toServer.maybeReconnect();
    toServer.write("ping\n");
    toServer.flush();
    toServer.write("give_rst\n");
    toServer.flush();
    toServer.maybeReconnect();
    toServer.write("ping\n");

    Assert.assertEquals(2, connects);
  }
}