package org.myprog;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
//import java.util.List;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.List;
import java.util.StringTokenizer;

//import SortFile.Entity;




public class ExternalSort {

	
	public static void main(String[] args) {
		
		ExternalSort externalsort = new ExternalSort();
		String Path1 = "e://chromdown/int1" ;
		String Path2 = "e://pr_int/LongDes23" ;
		String PathOut = "e://pr_int/LongDesFinal" ;
		//String Path1 = "e://pr_int/2.txt" ;
		//String Path2 = "e://pr_int/1.txt" ;
		//String PathOut = "e://pr_int/3.txt" ;
		externalsort.readFile(Path1,Path2,PathOut);
		

	}
	
	public void readFile(String pathA, String pathB,String pathOut){
    	
        File file1 = new File(pathA);
        File file2 = new File(pathB);
        File fileOut = new File(pathOut);
        
       /*
        
        if(fileOut.exists()){
            System.out.println("File Exist! Quit This Program.");
            return;
        } */
        
        if(fileOut.exists()){
        	fileOut.delete();
        }
        
        try {
        	BufferedWriter writer = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(pathOut)));
        
        
       
        	
            System.out.println("-------------Begin to read the file---------------");
            
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(new FileInputStream(file1)));
            String readLine1 = null ;
            
            BufferedReader reader2 = new BufferedReader(new InputStreamReader(new FileInputStream(file2)));
            String readLine2 = null ;
            
            String PR1="";
            String PR2="";
            String PAGE1="";
            String PAGE2="";
            int PR_INT1 =-1;
            int PR_INT2 =-1;
            
            readLine1=reader1.readLine();
            readLine2=reader2.readLine();
            
            while(readLine1 != null )
            {
      
                readLine1 = readLine1.trim();
                
                StringTokenizer token1 = new StringTokenizer(readLine1, ",");
                
                if(token1.hasMoreTokens())
                {
                	PR1 = token1.nextToken().trim();
                	PR_INT1 = Integer.parseInt(PR1);
                	//System.out.println("1@"+PR_INT1);
                }
                
                if(token1.hasMoreTokens())
                {
                	PAGE1 = token1.nextToken().trim();
                	//System.out.println("1&"+PAGE1);
                }
                
                if(readLine2 == null)
                	break;

                while(readLine2 != null )
                {
	                readLine2 = readLine2.trim();
	                StringTokenizer token2 = new StringTokenizer(readLine2, ",");
	                    
	                if(token2.hasMoreTokens())
	                {
	                    PR2 = token2.nextToken().trim();
	                    PR_INT2 = Integer.parseInt(PR2);
	                    //System.out.println("2@"+PR_INT2);
	                }
	                    
	                if(token2.hasMoreTokens())
	                {
	                    PAGE2 = token2.nextToken().trim();
	                    //System.out.println("2&"+PAGE2);
	                }
	                    
	                if(PR_INT1 > PR_INT2 )
	                {
	                    String content = PR_INT1 + " , " + PAGE1 +  System.getProperty("line.separator"); ;
	                    writer.write(content);
	                    readLine1=reader1.readLine();
	                    break;
	                } else {
	                    String content = PR_INT2 + " , " + PAGE2 +  System.getProperty("line.separator"); ;
	                    writer.write(content);
	                    readLine2=reader2.readLine();
	                }	
	                    
                }
                   // System.in.read();
            }
               
            
            while( readLine1 != null )
            {	
            	writer.write(readLine1 + System.getProperty("line.separator") ); 
            	readLine1= reader1.readLine();
            } 
            
            while( readLine2 != null )
            {	
                writer.write(readLine2 + System.getProperty("line.separator")  );
                readLine2= reader2.readLine();
            }
                
            writer.close();
            reader1.close();
            reader2.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Writing Finished!");
    }
}

