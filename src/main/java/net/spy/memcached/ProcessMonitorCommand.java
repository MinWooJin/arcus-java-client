package net.spy.memcached;

import net.spy.memcached.compat.SpyThread;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Scanner;

public class ProcessMonitorCommand extends SpyThread implements Runnable {
  private InputStream is;
  private OutputStream os;
  private MonitorServer server;
  private long userDuration = -1;
  private int userLevel = -1;

  public ProcessMonitorCommand(InputStream i, OutputStream o, MonitorServer s) {
    is = i;
    os = o;
    server = s;
  }

  @Override
  public void run() {
    try {
      Scanner in = new Scanner(is);
      PrintWriter out = new PrintWriter(os, true);

      boolean done = false;
      while (!done && in.hasNextLine()) {
        // read data from Socket
        String line = in.nextLine();

        if (Command.QUIT.equals(line)) {
          done = true;
        } else {
          doCommand(line, out);
        }
      }

      in.close();
      is.close();
      os.close();
    } catch(IOException ex) {
      getLogger().error(ex.getMessage());
    }
  }

  private void doCommand(String line, PrintWriter out) {
    if (Command.START.equals(line)) {
      int ret = parseStartArgument(line);
      if (ret == 0) {
        if (server.startMonitor(server.getDuration(), server.getLevel())) {
          out.println("SUCCESS");
        } else {
          out.println("ALREADY STARTED");
        }
      } else if (ret == 1) {
        if (server.startMonitor((userDuration == -1 ? server.getDuration() : userDuration),
                (userLevel == -1 ? server.getLevel() : userLevel))) {
          out.println("SUCCESS");
        } else {
          out.println("ALREADY STARTED");
        }
      } else {
        assert ret < 0;
        out.println("INVALID ARGUMENT");
      }
    } else if (Command.STOP.equals(line)) {
      server.stopMonitor();
      out.println("SUCCESS");
    } else if (Command.STATE.equals(line)) {
      if (server.getMonitor() != null) {
        out.println("MONITOR RUNNING");
      } else {
        out.println("MONITOR NOT RUNNING");
      }
    } else if (Command.HELP.equals(line)) {
      out.println("monitor start <duration:by second> <level: 0 (CPU, GC), 1 (CPU, GC, OP:loose), 2(CPU, GC, OP:tight>");
      out.println("monitor stop");
      out.println("monitor state");
    } else {
      out.println("INVALID COMMAND");
    }
    out.flush();
  }

  private int parseStartArgument(String command) {
    String[] splittedCommand = command.split(" ");
    System.out.println(splittedCommand.length);
    if (splittedCommand.length == 2) {
      return 0; /* use default duration and level */
    } else if (splittedCommand.length == 4) {
      userDuration = Long.valueOf(splittedCommand[2]);
      userLevel = Integer.valueOf(splittedCommand[3]);
      if (userDuration <= 0 || (userLevel < 0 || userLevel > 2)) {
        /* invalid arguments */
        return -1;
      }
      return 1; /* use user duration and level */
    }
    return -1;
  }

  enum Command {
    QUIT("quit"),
    HELP("help"),

    /* monitor command */
    STATE("state"),
    START("start"),
    STOP("stop");


    private String command;

    Command(String command) {
      this.command = command;
    }

    public boolean equals(String command) {
      String[] splittedCommand = command.split(" ");
      if (splittedCommand.length > 0) {
        if (splittedCommand.length == 1) {
          return splittedCommand[0].equalsIgnoreCase(this.command);
        } else {
          return splittedCommand[0].equalsIgnoreCase("monitor") && splittedCommand[1].equalsIgnoreCase(this.command);
        }
      }
      return false;
    }
  }
}
