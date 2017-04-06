package com.wavefront.integrations;

import java.io.IOException;

/**
 * Driver class for ad-hoc experiments with {@link WavefrontSender}
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class Main {

  public static void main(String[] args) throws InterruptedException, IOException {
    String host = args[0];
    String port = args[1];
    System.out.println(host + ":" + port);
    Wavefront wavefront = new Wavefront(host, Integer.parseInt(port));
    while (true) {
      wavefront.send("mymetric.foo", 42);
      wavefront.flush();
      Thread.sleep(2000);
    }
  }
}
