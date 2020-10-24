import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.LinkedList;  //best practice queue
import java.util.Queue; //best practice queue dibuat dengan Queue q = new LinkedList();
import java.lang.Thread;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.Serializable;
import java.util.List;

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
  static int runningFlag = 0; //active + running, active + idle, dead

  //run by typing "java Worker IP port". //args = port
  public static void main(String args[])throws Exception{ 
    prepareWorker(args[0]);
    startConnection();
    
    //while true ini masih sementara. nantinya pake thread
    while(true){
      print("Waiting for job...");
      String input = receiveCommand();
      print("Order Received. Input:");
      print("Sorting...");
      doSort();
      returnJob();
      print("Result has been returned to master.");
      if(input.equals("stop")) break;
    }  
    
    endConnection();
  }
  
  //format sort = "sort id | array csv"
  //menerima dan mendefine perintah dari master
  static String receiveCommand() throws Exception{
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
        //doStop
        break;
    }
    return input;
  }
  
  //persiapan sebelum sort (mengambil array dari format perintah dari master) dan sorting
  static void doSort() {        
    if(jobsQueue.peek() != null) {
      String input = jobsQueue.poll();
      int borderIdx = input.indexOf("#");
      String command = input.substring(0,borderIdx);
      String array = input.substring(borderIdx+1, input.length());
      runningFlag = Integer.parseInt(command.split(" ")[1]);
      
      String sorted = sort(array);
      
      String finalResult = command + "#" + sorted;
      runningFlag = 0;
      finishedJobsQueue.offer(finalResult);
    }
  }
  
  //mengirim hasil ke master
  static void returnJob() throws Exception{
      if(finishedJobsQueue.peek() != null) {
        String toBeReturned = finishedJobsQueue.poll();
        dout.writeUTF(toBeReturned);
        dout.flush();
      }
  }
  
  static void prepareWorker(String port) throws Exception {
    ss = new ServerSocket(Integer.parseInt(port));
    jobsQueue = new LinkedList<String>();
    finishedJobsQueue = new LinkedList<String>();
  }
  
  static void startConnection() throws Exception{
    print("Connecting...");
    s = ss.accept();
    din = new DataInputStream(s.getInputStream());  
    dout=new DataOutputStream(s.getOutputStream());       
    print(
      "                      __\n"+
      " _      ______  _____/ /_____  _____\n"+
      "| | /| / / __ \\/ ___/ //_/ _ \\/ ___/\n"+
      "| |/ |/ / /_/ / /  / ,< /  __/ /\n"+
      "|__/|__/\\____/_/  /_/|_|\\___/_/\n"+
      "\n"//+
      //String.format("Connected to %s:%d",ip,port)
    );
  }
  
  static void endConnection() throws Exception{
    dout.close();
    dout.close();  
    s.close();
    print("Worker is successfully stopped."); 
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
    for (var entry: freq.entrySet()) {
      int value = entry.getValue();
      while (value-- > 0) {
        toSort[i++] = entry.getKey();
      }
    }
    // Returns the String repr of the sorted array
    return Arrays.toString(toSort);
  }
} 