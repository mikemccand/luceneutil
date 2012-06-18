package perf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

// Serves up tasks from remote client
class RemoteTaskSource extends Thread implements TaskSource {
  private final ServerSocket serverSocket;
  private final TaskParser taskParser;
  private final int numThreads;
  private static final int MAX_BYTES = 70;

  // nocommit maybe fair=true?
  private final BlockingQueue<Task> queue = new ArrayBlockingQueue<Task>(100);

  public RemoteTaskSource(String iface, int port, int numThreads, TaskParser taskParser) throws IOException {
    this.numThreads = numThreads;
    this.taskParser = taskParser;
    serverSocket = new ServerSocket(port, 50, InetAddress.getByName(iface));
    System.out.println("Waiting for client connection on interface " + iface + ", port " + port);
    setPriority(Thread.MAX_PRIORITY);
    setDaemon(true);
    start();
  }
  
  @Override
  public List<Task> getAllTasks() {
    return null;
  }

  private volatile OutputStream out;

  @Override
  public void run() {
    // Start server socket and accept only one client
    // connection, which will feed us the requests:

    newClient: while(true) {
      Socket socket = null;
      InputStream in;
      try {
        System.out.println("  ready for client...");
        socket = serverSocket.accept();
        in = socket.getInputStream();
        out = socket.getOutputStream();
      } catch (IOException ioe) {
        if (socket != null) {
          try {
            socket.close();
          } catch (IOException ioe2) {
          }
        }
        continue;
      }
      System.out.println("    connection!");

      try {
        final byte[] buffer = new byte[MAX_BYTES];
        int taskCount = 0;
        while(true) {
          int upto = 0;
          while(upto < buffer.length) {
            final int inc;
            try {
              inc = in.read(buffer, upto, MAX_BYTES-upto);
            } catch (java.net.SocketException se) {
              socket.close();
              queue.clear();
              out = null;
              continue newClient;
            }
            if (inc >= 0) {
              upto += inc;
            } else {
              // Connection closed
              socket.close();
              queue.clear();
              out = null;
              continue newClient;
            }
          }

          String s = new String(buffer, "UTF-8");
          if (s.startsWith("END//")) {
            for(int threadID=0;threadID<numThreads;threadID++) {
              queue.put(Task.END_TASK);
            }
            break;
          }
          Task task;
          try {
            task = taskParser.parseOneTask(s);
          } catch (RuntimeException re) {
            re.printStackTrace();
            continue;
          }
          task.recvTimeNS = System.nanoTime();
          task.taskID = taskCount++;
          queue.put(task);
          //System.out.println("S: add " + s + "; size=" + queue.size());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Task nextTask() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void taskDone(Task task, long queueTimeNS) throws IOException {
    if (out != null) {
      try {
        // NOTE: can cause NPE here (we are not sync'd)
        // but caller will print & ignore it...
        out.write(String.format("%8d:%7.1f", task.taskID, queueTimeNS/1000000.0).getBytes("UTF-8"));
      } catch (SocketException se) {
        System.out.println("Ignore SocketException: " + se);
        queue.clear();
      } catch (UnsupportedEncodingException uee) {
        throw new RuntimeException(uee);
      }
    }
  }
}

