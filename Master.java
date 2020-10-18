import java.net.*;  
import java.io.*;  
class Master{  
    public static void main(String args[])throws Exception{  
        ServerSocket ss=new ServerSocket(3333);
        print("Waiting for worker");
        Socket s=ss.accept();
        // Socket s2=ss.accept();
        // Socket s3=ss.accept();
        DataInputStream din=new DataInputStream(s.getInputStream());  
        DataOutputStream dout=new DataOutputStream(s.getOutputStream());  
        BufferedReader br=new BufferedReader(new InputStreamReader(System.in));  
        String result="",order="";  
        while(!order.equals("stop")){ 
            System.out.println("Go");
            order=br.readLine();
            dout.writeUTF(order);
            dout.flush();
            result=din.readUTF();
            System.out.println("Worker says: "+ result);
        }
        din.close();  
        s.close();  
        ss.close();  
    }
    static void print(String s){System.out.println(s);}
}