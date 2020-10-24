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
//print
//seeder
class Master{
  static Random rand = new Random();
  static ServerSocket ss;
  static ArrayList<Socket> socketList;
  static ArrayList<DataInputStream> dinList;
  static ArrayList<DataOutputStream> doutList;
  static ArrayList<ArrayList<Integer>> workerJobInfo;
  static Queue<String> jobsQueue;
  static Queue<String> finishedJobsQueue;
  static BufferedReader shellReader;
  static boolean stopFlag;

  //format sort = "sort id # array", id = xxxyyy, xxx=kode perequest (dari shell = 000), yyy=kode job keberapa
  public static void main(String args[]) throws Exception {//argumen 0 = port server socket
    prepareMaster(args[0]);
    print("Waiting for worker");
    connectWorker();//nantinya ini pake thread dan while true supaya bisa nerima banyak worker
    connectWorker();
    print("Connection with worker has been established successfully.");
    
    while (stopFlag!=true) {
      print("Waiting for job for worker.");
      readShellCommand();
      readShellCommand();
      distributeJob();
      print("Job given to Worker. Waiting for result...");
      receiveFinishedJob(0);
      receiveFinishedJob(1);
      print("Result: " + finishedJobsQueue.poll());
      print("Result: " + finishedJobsQueue.peek());
      
    }
    
    disconnectWorker(0);
    ss.close();
    print("Master is successfully stopped.");
  }
  
  static void prepareMaster(String port) throws Exception {
    ss = new ServerSocket(Integer.parseInt(port));
    socketList = new ArrayList<Socket>();
    dinList = new ArrayList<DataInputStream>();
    doutList = new ArrayList<DataOutputStream>();
    workerJobInfo = new ArrayList<ArrayList<Integer>>();
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

  static void connectWorker() throws Exception {
    Socket tmpSocket = ss.accept();
    socketList.add(tmpSocket);
    dinList.add(new DataInputStream(tmpSocket.getInputStream()));
    doutList.add(new DataOutputStream(tmpSocket.getOutputStream()));
    workerJobInfo.add(new ArrayList<Integer>());
    print("new worker connected");
  }
  
  static void disconnectWorker(int workerID) throws Exception {
    Socket tmpS = socketList.remove(workerID);
    DataInputStream tmpDIn = dinList.remove(workerID);
    DataOutputStream tmpDOut = doutList.remove(workerID);
    workerJobInfo.remove(workerID);
    
    tmpS.close();
    tmpDIn.close();
    tmpDOut.close();
  }
  
  static void readShellCommand() throws Exception {//nanti ada receive command buat nerima perintah dari user socke
    String input = shellReader.readLine();
    String command = input.length()>12 ? input.substring(0,12).split(" ")[0] : input.split(" ")[0];
    switch(command) {
      case "generate" :
        input = "sort 000000 #" + seeder(input.split(" ")[1]); //nanti harusnya ada fungsi buat generate id nya
        //no break karna abis di generate akan di sort juga
      case "sort" : 
        jobsQueue.add(input);
        
        break;
      case "stop" :
        stopFlag = true;
        break;
    }
  }
  
  static void distributeJob() throws Exception{//load balancer
    //job distribution start from worker with lowest job
    int workerIdx = getWorkerWithLowestQueue();
    int tmpLimit = jobsQueue.size();
    for(int jobsIdx = 0; jobsIdx<tmpLimit; jobsIdx++) {
      print(workerIdx);
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
  
  static void receiveFinishedJob(int workerID) throws Exception  {
    DataInputStream tmpDIn = dinList.get(workerID);
    String finishedJob = tmpDIn.readUTF();
    
    //hapus ID job ini dari list job worker itu //btw ini bisa pake cara yg sama dengan di sendjob, cuma tadi pas nulis lupa format finishedJob kaya apa
    String jobId = finishedJob.substring(0,20).split(" ")[1];
    workerJobInfo.get(workerID).remove(jobId);
    
    finishedJobsQueue.add(finishedJob);
  }
  
  static void print(String s) {
    System.out.println(s);
  }
  
  static void print(int i) {
    System.out.println(i);
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
