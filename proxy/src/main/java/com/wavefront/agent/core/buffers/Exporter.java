package com.wavefront.agent.core.buffers;

import static com.wavefront.agent.core.buffers.ActiveMQBuffer.MSG_GZIPBYTES;

import com.wavefront.data.ReportableEntityType;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;

public class Exporter {
  public static void export(String bufferStr, String dirStr, String atomsStr, boolean retainData) {
    List<String> atomsList = Arrays.asList(atomsStr.split(","));
    atomsList.replaceAll(String::trim);
    List<ReportableEntityType> atoms =
        atomsList.stream()
            .map(
                s -> {
                  ReportableEntityType atom = ReportableEntityType.fromString(s);
                  if (atom == null) {
                    throw new IllegalArgumentException("invalid atom '" + s + "'");
                  }
                  return atom;
                })
            .collect(Collectors.toList());
    File dir = new File(dirStr);

    DiskBufferConfig config = new DiskBufferConfig();
    config.buffer = new File(bufferStr);
    DiskBuffer buffer = new DiskBuffer(1, "disk", config);
    atoms.forEach(
        atom -> {
          ActiveMQServer amq = buffer.activeMQServer;
          try {
            File outFile = new File(dir, atom.toString().toLowerCase() + ".txt");
            System.out.println(
                "Exporting '" + atom + "' from '" + dirStr + "' to '" + outFile + "'");
            AtomicInteger c = new AtomicInteger();
            BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
            amq.getPostOffice()
                .listQueuesForAddress(SimpleString.toSimpleString(atom.name()))
                .forEach(
                    queue -> {
                      LinkedListIterator<MessageReference> it = queue.browserIterator();
                      while (it.hasNext()) {
                        CoreMessage msg = (CoreMessage) it.next().getMessage();
                        String str = "";
                        if (msg.getIntProperty(MSG_GZIPBYTES) != 0) {
                          str = GZIP.decompress(msg);
                        } else {
                          str = msg.getReadOnlyBodyBuffer().readString();
                        }
                        List<String> points = Arrays.asList(str.split("\n"));
                        try {
                          out.write(String.join("\n", points));
                          out.write("\n");
                        } catch (IOException e) {
                          throw new RuntimeException("Error writing on the output file.", e);
                        }
                        if (!retainData) {
                          try {
                            queue.deleteReference(msg.getMessageID());
                          } catch (Exception e) {
                            throw new RuntimeException("Error deleting data from the buffer", e);
                          }
                        }
                        if (c.addAndGet(points.size()) % 100_000 == 0) {
                          System.out.print(".");
                        }
                      }
                    });
            out.flush();
            out.close();
            System.out.println(
                ".\nDone, exported "
                    + (retainData ? "" : "and deleted ")
                    + c
                    + " "
                    + atom.toString()
                    + "\n");
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    buffer.shutdown();
  }
}
