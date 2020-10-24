import java.net.*;
import java.util.Arrays;
import java.util.Random;
import java.io.*;
import java.util.*;

//main
//prepareMaster
//connectWorker
//disconnectWorker
//readShellCommand
//distributeJob
//getWorkerWithLowestQueue
//sendJob
//receiveFinishedJob
//print
//seeder
class Master{
  static Random rand = new Random();
  static ArrayList<Socket> socketList;
  static ArrayList<DataInputStream> dinList;
  static ArrayList<DataOutputStream> doutList;
  static ArrayList<ArrayList<Integer>> workerJobInfo;
  static ArrayList<String> workerAddressInfo;
  static Queue<String> jobsQueue;
  static Queue<String> finishedJobsQueue;
  static BufferedReader shellReader;
  static boolean stopFlag = false;
  static boolean readingFlag = true;

  //format sort = "sort id # array", id = xxxyyy, xxx=kode perequest (dari shell = 000), yyy=kode job keberapa
  public static void main(String args[]) throws Exception {//argumen 0 = port server socket
    prepareMaster();
    print("Waiting for worker");
    while(true){
      printsln("Worker address: ");
      String wAddr  = shellReader.readLine();
      if(wAddr.equals("X")) break;  // stop menerima addr worker

      else if(wAddr.equals("default")){
        connectWorker("34.224.75.203:3333");
        connectWorker("34.224.75.203:3334");
        print("Connection with worker has been established successfully.");
        break;
      }

      try {
        connectWorker(wAddr);
      } catch (Exception e) {
        print("Worker not found, try again.");
      }
    }

    while (true) {  
      print("Waiting for orders.");
      print(
        "List of orders:"+
        "sort xxxyyy #array (Sort the given array at #),\n"+
        "generate n (Generate a random int array of size n), \n"+
        "addWorker ipAddress:port (Adds a new worker),\n"+
        "finish (stops asking inputs and distributes job to all connected worker),\n"+        
        "stop (stops the master and closes all connected worker)."
      );
      while(readingFlag){
        printsln("Order: ");
        readShellCommand();
      }
      if(stopFlag) break;
      distributeJob();
      print("Job given to Worker. Waiting for result...");
      receiveFinishedJob();
      print("Result: " + finishedJobsQueue.poll());
      print("Result: " + finishedJobsQueue.peek());
    }
    
    disconnectWorker(0);
    print("Master is successfully stopped.");
  }
  
  static void prepareMaster() throws Exception {
    socketList = new ArrayList<Socket>();
    dinList = new ArrayList<DataInputStream>();
    doutList = new ArrayList<DataOutputStream>();
    workerJobInfo = new ArrayList<ArrayList<Integer>>();
    workerAddressInfo = new ArrayList<String>();
    jobsQueue = new LinkedList<String>();
    finishedJobsQueue = new LinkedList<String>();
    shellReader = new BufferedReader(new InputStreamReader(System.in));
    print(
      "                         __\n"+
      "   ____ ___  ____ ______/ /____  _____\n"+
      "  / __ `__ \\/ __ `/ ___/ __/ _ \\/ ___/\n"+
      " / / / / / / /_/ (__  ) /_/  __/ /\n"+
      "/_/ /_/ /_/\\__,_/____/\\__/\\___/_/\n"+
      "\n"
    );
  }

  //nanti fungsi upscale downscale make ini dengan manfaatin variable workerAddressInfo
  static void connectWorker(String workerAddress) throws Exception {//format input = ipAddress:port
    try {
      String workerIP = workerAddress.split(":")[0];
      int workerPort = Integer.parseInt(workerAddress.split(":")[1]);
      Socket tmpSocket = new Socket(workerIP, workerPort);
      socketList.add(tmpSocket);
      dinList.add(new DataInputStream(tmpSocket.getInputStream()));
      doutList.add(new DataOutputStream(tmpSocket.getOutputStream()));
      workerJobInfo.add(new ArrayList<Integer>());
      print(String.format("Connected to worker s%", workerAddress));
    }
    catch (Exception e) {
    }
  }
  
  static void disconnectWorker(int workerIdx) throws Exception {
    Socket tmpS = socketList.remove(workerIdx);
    DataInputStream tmpDIn = dinList.remove(workerIdx);
    DataOutputStream tmpDOut = doutList.remove(workerIdx);
    workerJobInfo.remove(workerIdx);
    
    tmpS.close();
    tmpDIn.close();
    tmpDOut.close();
  }
  
  static void readShellCommand() throws Exception {//nanti ada receive command buat nerima perintah dari user socket
    String input = shellReader.readLine();
    String command = input.length()>12 ? input.substring(0,12).split(" ")[0] : input.split(" ")[0];
    switch(command) {
      case "generate" : //format : generate int
        input = "sort 000000 #" + seeder(input.split(" ")[1]); //nanti harusnya ada fungsi buat generate id nya
        //no break karna abis di generate akan di sort juga
      case "sort" : //format : sort xxxyyy # array
        jobsQueue.add(input);
        break;
      case "stop" : //format : stop
          stopFlag = true;
          break;
      case "finish" : //format : finish
          readingFlag = false;
          break;
      case "addWorker" : //format : addWorker ipAddress:port
        connectWorker(input.split(" ")[1]);
        // workerAddressInfo.add(input.split(" ")[1]);
    }
  }
  
  static void distributeJob() throws Exception{//load balancer
    //job distribution start from worker with lowest job
    int workerIdx = getWorkerWithLowestQueue();
    int tmpLimit = jobsQueue.size();
    for(int jobsIdx = 0; jobsIdx<tmpLimit; jobsIdx++) {
      if(workerIdx == workerJobInfo.size()) { workerIdx = 0; }
      sendJob(workerIdx);
      workerIdx++;
    }
  }
  
  static int getWorkerWithLowestQueue() {
    int lowestQIdx = 0;
    for(int i=1; i<workerJobInfo.size(); i++) {
      if(workerJobInfo.get(i).size()<workerJobInfo.get(lowestQIdx).size()) {
        lowestQIdx = i;
      }
      if(workerJobInfo.get(lowestQIdx).size() == 0) {
        break;
      }
    }
    return lowestQIdx;
  }

  static void sendJob(int workerIdx) throws Exception   {
    String job = jobsQueue.poll();
    
    //tambahkan id job ini ke list job worker itu
    String jobId = job.substring(5,10);
    workerJobInfo.get(workerIdx).add(Integer.parseInt(jobId)); 
    
    DataOutputStream tmpDOut = doutList.get(workerIdx);
    tmpDOut.writeUTF(job);
    tmpDOut.flush();
  }
  
  static void receiveFinishedJob() throws Exception  {
    for(int dinIdx = 0; dinIdx<dinList.size(); dinIdx++) {
      if(dinList.get(dinIdx) == null) {continue;} //socket yg lagi dimatiin di null in semua variable socket nya kecuali workerAddressInfo
      DataInputStream tmpDIn = dinList.get(dinIdx);
      String finishedJob = tmpDIn.readUTF();
      
      //hapus ID job ini dari list job worker itu //btw ini bisa pake cara yg sama dengan di sendjob, cuma tadi pas nulis lupa format finishedJob kaya apa
      String jobId = finishedJob.substring(0,20).split(" ")[1];
      workerJobInfo.get(dinIdx).remove(jobId);
      
      finishedJobsQueue.add(finishedJob);
    }
  }
  
  static void print(String s) {
    System.out.println(s);
  }
  
  static void print(int i) {
    System.out.println(i);
  }

  static void printsln(String s) {
    System.out.print(s);
  }

  static String seeder(String size) {
    int max = Integer.parseInt(size);
    int[] job = new int[max];
    for (int i = 0; i < max; i++) {
      job[i] = rand.nextInt(max+1);
    }
    return Arrays.toString(job);
  }
}
