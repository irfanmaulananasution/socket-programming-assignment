import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

class Worker{  
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
        while(true){
            // Input
            print("Waiting for job...");
            input=din.readUTF();  
            print("Order Received. Input:");
            print(input);
            if(input.equals("stop")) break;
            print("Sorting...");

            // Output
            dout.writeUTF(sort(input));  
            dout.flush();  
            print("Result has been returned to master.");
        }  
        din.close();
        dout.close();  
        s.close();  
        print("Worker is successfully stopped.");
    }

    static void print(String s){System.out.println(s);}

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
