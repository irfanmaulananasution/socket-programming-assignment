import java.net.*;
import java.util.Arrays;
import java.util.Random;
import java.io.*;

class Master{
    static Random rand = new Random();

    public static void main(String args[]) throws Exception {
        ServerSocket ss = new ServerSocket(3333);
        print("Waiting for worker");
        Socket s = ss.accept();
        // Socket s2=ss.accept();
        // Socket s3=ss.accept();
        DataInputStream din = new DataInputStream(s.getInputStream());
        DataOutputStream dout = new DataOutputStream(s.getOutputStream());
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        print("Connection with worker has been established successfully.");
        String result = "", order = "";
        while (true) {
            print("Waiting for job for worker.");
            order = br.readLine();
            if (order.equals("stop"))
                break;
            else if (order.split(" ")[0].equals("generate"))
                order = seeder(order.split(" ")[1]);
            dout.writeUTF(order);
            dout.flush();
            print("Job given to Worker. Waiting for result...");
            result = din.readUTF();
            print("Result: " + result);
        }
        din.close();
        s.close();
        ss.close();
        print("Master is successfully stopped.");
    }

    static void print(String s) {
        System.out.println(s);
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
