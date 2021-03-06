import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.LinkedList;  //best practice queue
import java.util.Queue; //best practice queue dibuat dengan Queue q = new LinkedList();
import java.lang.Thread;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

//main
//receiveCommand
//doSort
//returnJob
//startConnection
//endConnection
//print
//print
//sort
class Worker{  
  static ServerSocket ss;
  static Socket s;
  static DataInputStream din;
  static DataOutputStream dout;
  static Queue<String> jobsQueue;
  static Queue<String> finishedJobsQueue;
  static String runningFlag = "idle"; //active + running, active + idle, dead
  static List<WorkerHelper> workerHelperList;
  static ExecutorService pool;

  //run by typing "java Worker IP port". //args = port
  public static void main(String args[])throws Exception{ 
    prepareWorker(args[0]);
    
    //while true ini masih sementara. nantinya pake thread
    while(true){
      try{
        print("Waiting for command");
        receiveCommand();
        print("Command accepted");
        print("");
      }
      catch (SocketException e) {
        print("Socket Error");
        endConnection();
        startConnection();
      }
    }
  }
  
  //format sort = "sort id | array csv"
  //menerima dan mendefine perintah dari master
  static void receiveCommand() throws Exception{
    String input = din.readUTF();
    
    String command;
    if(input.length()>12) {
      command = input.split("#")[0];  //ini mungkin agak lama lebih cepet kalo di substring dulu karna dia bakal iterasi semua digit sampe akir
    }
    else {
      command = input;
    }
    
    switch(command.split(" ")[0]) {
      case "sort" : 
        jobsQueue.add(input);
        break;
      case "stop" :
        endConnection();
        startConnection();
        break;
      case "status" :
        returnStatusToMaster();
        //sendRunningFlag

    }
  }
  
  //persiapan sebelum sort (mengambil array dari format perintah dari master) dan sorting
  static void doSort() {
    if(jobsQueue.peek() != null) {
      String input = jobsQueue.poll();
      int borderIdx = input.indexOf("#");
      String command = input.substring(0,borderIdx);
      String array = input.substring(borderIdx+1, input.length());
      runningFlag = command.split(" ")[1];
      
      String sorted = sort(array);
      
      String finalResult = command + "#" + sorted;
      runningFlag = "idle";
      finishedJobsQueue.offer(finalResult);
    }
  }
  
  //mengirim hasil ke master
  static void returnJobToMaster() throws Exception{
    if(finishedJobsQueue.peek() != null) {
      print("RETURNING SOMETHING");
      String toBeReturned = finishedJobsQueue.poll();
      dout.writeUTF(toBeReturned);
      dout.flush();
    }
  }
  
  static void returnStatusToMaster() throws Exception{
    String toBeReturned = runningFlag;
    dout.writeUTF(toBeReturned);
    dout.flush();
  }
  
  static void prepareWorker(String port) throws Exception {
    print(
      "                      __\n"+
      " _      ______  _____/ /_____  _____\n"+
      "| | /| / / __ \\/ ___/ //_/ _ \\/ ___/\n"+
      "| |/ |/ / /_/ / /  / ,< /  __/ /\n"+
      "|__/|__/\\____/_/  /_/|_|\\___/_/\n"+
      "\n"+
      "waiting for Master"
      //String.format("Connected to %s:%d",ip,port)
    );
    ss = new ServerSocket(Integer.parseInt(port));
    jobsQueue = new LinkedList<String>();
    finishedJobsQueue = new LinkedList<String>();
    workerHelperList = new ArrayList<WorkerHelper>();

    startConnection();

  }
  
  static void startConnection() throws Exception{
    print("Listening");
    s = ss.accept();
    din = new DataInputStream(s.getInputStream());  
    dout=new DataOutputStream(s.getOutputStream());
    pool = Executors.newFixedThreadPool(2);
    WorkerHelper doSortHelper = new WorkerHelper("doSort");
    WorkerHelper returnJobHelper = new WorkerHelper("returnJob");
    workerHelperList.add(doSortHelper);
    workerHelperList.add(returnJobHelper);
    pool.execute(doSortHelper);
    pool.execute(returnJobHelper);
    
    print("Master connected");
  }
  
  static void endConnection() throws Exception{
    while(jobsQueue.isEmpty() == false || finishedJobsQueue.isEmpty() == false) {
      Thread.sleep(1000);
    }
    pool.shutdownNow();
    workerHelperList.clear();
    din.close();
    dout.close();  
    s.close();
    print("Master disconnected"); 
  }
  
  static void print(String s){System.out.println(s);}

  static void print(int i){System.out.println(i);}

  static String sort(String input){
    String[] toParse = input.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "").split(",");

    int[] toSort = new int[toParse.length];

    // Parses the input, checking if there's non number in the values.
    for (int i = 0; i < toSort.length; i++) {
      try {
        toSort[i] = Integer.parseInt(toParse[i]);
      } catch (NumberFormatException nfe) {
        //NOTE: write something here if you need to recover from formatting errors
        String err = "The array has invalid value: "+toParse[i]+" at index "+Integer.toString(i);
        print(err);
        return err;
      };
    }
    // Sorting algorithm used: Counting sort with Treemap for sorting and storing duplicate values
    // Source: https://www.techiedelight.com/efficiently-sort-array-duplicated-values/
    Map<Integer, Integer> freq = new TreeMap<>();

    // store distinct values in the input array as key and their respective counts as values
    for (int i: toSort) {
      freq.put(i, freq.getOrDefault(i, 0) + 1);
    }

    // traverse the sorted map and overwrite the input array with sorted elements
    int i = 0;
    for (Map.Entry<Integer, Integer>  entry : freq.entrySet()) {
      int value = entry.getValue();
      while (value-- > 0) {
        toSort[i++] = entry.getKey();
      }
    }
    // Returns the String repr of the sorted array
    return Arrays.toString(toSort);
  }
}

class WorkerHelper implements Runnable{
  String handlerTask;

  WorkerHelper(String command) {
    this.handlerTask = command;
  }

  @Override
  public void run() {
    try {
      if (handlerTask.equals("doSort")) {
        while(true) {
          Worker.doSort();
          Thread.sleep(1);
        }
      }
      if (handlerTask.equals("returnJob")) {
        while(true) {
          Worker.returnJobToMaster();
          Thread.sleep(50);
        }

      }
    }
    catch (Exception e) {
    }
  }
}