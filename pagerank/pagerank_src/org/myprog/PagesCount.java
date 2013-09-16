package org.myprog;

import java.io.*;
//import java.util.*;
//import java.io.File;
//import java.io.IOException;


public class PagesCount {
	
	public static void main(String[] args) throws Exception {
	
		long i=0;
		
		//BufferedReader rb = new BufferedReader(new FileReader("e://part/part-00000"));
	
		BufferedReader rb = new BufferedReader(new FileReader("e://pr_int/LongDesFinal")); 
		//BufferedReader rb = new BufferedReader(new FileReader("hdfs://localhost:54310/user/hduser/input2/"));
	    
	    String temp_line=rb.readLine(); 
	       
	    while( temp_line != null){
	        i++;
	        temp_line=rb.readLine();
	    }
	       
	    System.out.println("The total number of pages is: "+ i);
	    
	    rb.close();
	}
}
