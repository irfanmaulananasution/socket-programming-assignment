import java.net.*;
import java.util.Arrays;
import java.util.Random;
import java.io.*;
import java.util.*;
import java.lang.Thread;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//main
//prepareMaster
//connectWorker
//disconnectWorker
//readShellCommand
//distributeJob
//getWorkerWithLowestQueue
//sendJob
//receiveFromWorker
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
  static List<MasterHelper> masterHelperList;
  static ExecutorService pool;


  //format sort = "sort id # array", id = xxxyyy, xxx=kode perequest (dari shell = 000), yyy=kode job keberapa
  public static void main(String args[]) throws Exception {//argumen 0 = port server socket
    prepareMaster();
    connectWorker(workerAddressInfo.get(0));
    println(
      "---------------List Of Command---------------\n"+
      ">help = show the command list\n"+
      ">sort [xxxyyy] #[array] = Sort the given array at #,\n"+
      ">generate [n] = Generate a random int array of size n, \n"+ 
      ">stop = stops the master and closes all connected worker.\n"+
      ">addWorkerAddress [ipAddress:port] = Adds new address of worker to the list,\n"+    
      ">activateNewWorker [idx] = activate a worker (upscale),\n"+  
      ">deactivateWorker [idx] = deactivate a worker (downscale) ,\n"+   
      ">showWorkerAddressList = show all worker listed in the master whether its active or not\n"+
      ">showFinishedJob = show one finished job and how many job are finished\n"+
      "---------------List Of Command---------------"
    );
    
    while (stopFlag == false) {
      print("Input : ");
      readShellCommand();
    }
    
    disconnectWorker(0);
    println("Master is successfully stopped.");
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
    println(
      "                         __\n"+
      "   ____ ___  ____ ______/ /____  _____\n"+
      "  / __ `__ \\/ __ `/ ___/ __/ _ \\/ ___/\n"+
      " / / / / / / /_/ (__  ) /_/  __/ /\n"+
      "/_/ /_/ /_/\\__,_/____/\\__/\\___/_/\n"+
      "\n"
    );
    pool = Executors.newFixedThreadPool(2);
    masterHelperList = new ArrayList<MasterHelper>();
    MasterHelper distributeJobHelper = new MasterHelper("distributeJob");
    MasterHelper receiveFromWorkerHelper = new MasterHelper("receiveFromWorker");
    masterHelperList.add(distributeJobHelper);
    masterHelperList.add(receiveFromWorkerHelper);
    pool.execute(distributeJobHelper);
    pool.execute(receiveFromWorkerHelper);
    
    requestInitialWorkerAddress();
  }
  
  static void requestInitialWorkerAddress() throws Exception{
    print("You need to input all worker address first!. how many worker are prepared?");
    int workerNum = Integer.parseInt(shellReader.readLine());
    for(int i = 0; i<workerNum ; i++) {
      print("Worker address " + (i+1) + ": ");
      String workerAddress = shellReader.readLine();
      workerAddressInfo.add(workerAddress);
    }
    println(""+ workerNum + " Worker Address added to list");
  }

  //nanti fungsi upscale downscale make ini dengan manfaatin variable workerAddressInfo
  static void connectWorker(String workerAddress) throws Exception {//format input = ipAddress:port
      String workerIP = workerAddress.split(":")[0];
      int workerPort = Integer.parseInt(workerAddress.split(":")[1]);
      Socket tmpSocket = new Socket(workerIP, workerPort);
      socketList.add(tmpSocket);
      dinList.add(new DataInputStream(tmpSocket.getInputStream()));
      doutList.add(new DataOutputStream(tmpSocket.getOutputStream()));
      workerJobInfo.add(new ArrayList<Integer>());
      println(String.format("Connected to worker %s", workerAddress));
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
  
  static void showWorkerAddressList() {
    String result = "";
    for(int i = 0; i<workerAddressInfo.size(); i++) {
      result = result + i + ". " + workerAddressInfo.get(i) + "\n";
    }
    print(result);
  }
  
  static void readShellCommand() throws Exception {//nanti ada receive command buat nerima perintah dari user socket
    String input = shellReader.readLine();
    String command = input.length()>25 ? input.substring(0,25).split(" ")[0] : input.split(" ")[0];
    switch(command) {
      case "help" :        
        println(
          "---------------List Of Command---------------\n"+
          ">help = show the command list\n"+
          ">sort [xxxyyy] #[array] = Sort the given array at #,\n"+
          ">generate [n] = Generate a random int array of size n, \n"+ 
          ">stop = stops the master and closes all connected worker.\n"+
          ">addWorkerAddress [ipAddress:port] = Adds new address of worker to the list,\n"+    
          ">activateNewWorker [idx] = activate a worker (upscale),\n"+  
          ">deactivateWorker [idx] = deactivate a worker (downscale) ,\n"+   
          ">showWorkerAddressList = show all worker listed in the master whether its active or not\n"+
          ">showFinishedJob = show one finished job and how many job are finished\n"+
          "---------------List Of Command---------------"
        );
        break;
      case "generate" : //format : generate int
        println("generate");
        input = "sort 000000 #" + seeder(input.split(" ")[1]); //nanti harusnya ada fungsi buat generate id nya
        //no break karna abis di generate akan di sort juga
      case "sort" : //format : sort xxxyyy # array
        println("sort");
        jobsQueue.add(input);
        break;
      case "stop" : //format : stop
        println("stop");
          stopFlag = true;
          break;
      case "addWorkerAddress" : //format : addWorkerAddress ipAddress:port
        println("addWorkerAddress");
        workerAddressInfo.add(input.split(" ")[1]);
        break;
      case "activateNewWorker" : //format : activateNewWorker idx
        println("activateNewWorker");
        int tmpIdx = Integer.parseInt(input.split(" ")[1]);
        connectWorker(workerAddressInfo.get(tmpIdx));
        break;
      case "deactivateWorker" : //format : deactivateWorker idx
        println("deactivateWorker");
        disconnectWorker(Integer.parseInt(input.split(" ")[1]));
        break;
      case "showWorkerAddressList" : //format : showWorkerAddressList
        println("showWorkerAddressList");
        showWorkerAddressList();
        break;
      case "showFinishedJob" : //format : showFinishedJob
        println("showFinishedJob");
        print(finishedJobsQueue.peek());
        println(" in Queue : " + finishedJobsQueue.size());
        break;
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

  static void commandWorker(int workerIdx, String command) throws Exception{
    DataOutputStream tmpDOut = doutList.get(workerIdx);
    tmpDOut.writeUTF(command);
    tmpDOut.flush();
  }
  
  static void receiveFromWorker() throws Exception  {
    for(int dinIdx = 0; dinIdx<dinList.size(); dinIdx++) {
      if(dinList.get(dinIdx) == null) {continue;} //socket yg lagi dimatiin di null in semua variable socket nya kecuali workerAddressInfo
      DataInputStream tmpDIn = dinList.get(dinIdx);
      String finishedJob = tmpDIn.readUTF();
      
      //hapus ID job ini dari list job worker itu //btw ini bisa pake cara yg sama dengan di sendjob, cuma tadi pas nulis lupa format finishedJob kaya apa
      String jobId = finishedJob.substring(0,20).split(" ")[1];
      if(jobId.equals("status")) {
        //doReportStatus
        break;
      }
      workerJobInfo.get(dinIdx).remove(jobId);
      
      finishedJobsQueue.add(finishedJob);
    }
  }
  
  static void println(String s) {
    System.out.println(s);
  }
  
  static void println(int i) {
    System.out.println(i);
  }

  static void print(String s) {
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

class MasterHelper implements Runnable{
  String handlerTask;

  MasterHelper(String command) {
    this.handlerTask = command;
  }

  @Override
  public void run() {
    try {
      if (handlerTask.equals("distributeJob")) {
        while(true) {
          Master.distributeJob();
          Thread.sleep(5);
        }
      }
      if (handlerTask.equals("receiveFromWorker")) {
        while(true) {
          Master.receiveFromWorker();
          Thread.sleep(5);
        }

      }
    }
    catch (Exception e) {
      System.out.println(e);
    }
  }
}
