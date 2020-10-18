import java.net.*;
import java.io.*;

class Master{  
    public static void main(String args[])throws Exception{
        BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
        print("Master IP: ");
        String masterIP = br.readLine();
        print("Master Port: ");
        int masterPort = Integer.parseInt(br.readLine());
        print("Connecting...");
        Socket s=new Socket(masterIP,masterPort);  
        DataInputStream din=new DataInputStream(s.getInputStream());  
        DataOutputStream dout=new DataOutputStream(s.getOutputStream());    
        print("Connected.");
        
        String input="";  
        while(!input.equals("stop")){  
            // Input
            print("Waiting for job...");
            input=din.readUTF();  
            print("Job Received. Input:");
            print(input);
            print("Sorting...");

            // Output
            dout.writeUTF(sort(input));  
            dout.flush();  
            print("Result has been returned to master.");
        }  
        dout.close();  
        s.close();  
    }
    static void print(String s){System.out.println(s);}
    static String sort(String input){
        return input;
    }
} 
