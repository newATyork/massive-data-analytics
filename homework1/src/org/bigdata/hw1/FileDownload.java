package org.bigdata.hw1;

import java.io.File;     
import java.net.URL;     
import org.apache.commons.io.FileUtils;     
    
public class FileDownload {  
	
	
	/*
     
    public static void main(String[] args) {     
    
        String res1 = downloadFromUrl("http://vgc.poly.edu/~juliana/courses/cs9223/Homework/HW1/hurricane-center-coordinates.csv","/usr/local/hadoop/");  
        System.out.println(res1);   
        
        String res2 = downloadFromUrl("http://vgc.poly.edu/~juliana/courses/cs9223/Homework/HW1/hurricane-center-addresses.csv","/usr/local/hadoop/");  
        System.out.println(res2);  
    }     
    */
    
    
    public static String downloadFromUrl(String url,String dir) {     
    
        try {     
            URL httpurl = new URL(url);     
            String fileName = getFileNameFromUrl(url);     
            //System.out.println(fileName);     
            File f = new File(dir + fileName);     
            FileUtils.copyURLToFile(httpurl, f);     
        } catch (Exception e) {     
            e.printStackTrace();     
            return "Fault!";     
        }      
        return "Successful!";     
    }     
         
    public static String getFileNameFromUrl(String url){     
        String name = "";   
        int index = url.lastIndexOf("/");  
        if(index > 0){     
            name = url.substring(index + 1);     
            if(name.trim().length()>0){
            	System.out.println(name+" is downloaded.");
                return name;     
            }     
        }  
        return name;    
    }     
}    