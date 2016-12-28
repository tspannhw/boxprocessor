package com.dataflowdeveloper;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;

public final class Main {
    private static final String DEVELOPER_TOKEN = "7PDSBQPLHpTlWfbH45jyIbL48yvxLGx0";
    private static final int MAX_DEPTH = 1;

    private Main() { }

    public static void main(String[] args) {
        // Turn off logging to prevent polluting the output.
        Logger.getLogger("com.box.sdk").setLevel(Level.ALL);

        BoxAPIConnection api = new BoxAPIConnection(DEVELOPER_TOKEN);

        BoxUser.Info userInfo = BoxUser.getCurrentUser(api).getInfo();
        System.out.format("Welcome, %s <%s>!\n\n", userInfo.getName(), userInfo.getLogin());

//        BoxFolder rootFolder = BoxFolder.getRootFolder(api);
//        listFolder(rootFolder, 0);

        BoxFile file = null;
        BoxFolder folder = new BoxFolder(api, "15296958056");
        for (BoxItem.Info itemInfo : folder) {
            if (itemInfo instanceof BoxFile.Info) {
                BoxFile.Info fileInfo = (BoxFile.Info) itemInfo;
                // Do something with the file.
                System.out.println("File:" + fileInfo.getCreatedAt() + "," +
                fileInfo.getDescription() + "," +
                fileInfo.getExtension() + ",name=" + 
                fileInfo.getName() + ",id=" + 
                fileInfo.getID() + "," +
                fileInfo.getCreatedBy() + "," + 
                fileInfo.getSize() + "," + 
                fileInfo.getVersion().getName() + "," + 
                fileInfo.getCreatedAt() + "," + 
                fileInfo.getModifiedAt() + "," + 
                fileInfo.getModifiedBy() + 
                "");
              
                
                // download all the pdfs
                if ( fileInfo.getName() != null && fileInfo.getID() != null && fileInfo.getName().endsWith(".pdf")) {
                	file = new BoxFile(api, fileInfo.getID());
                	FileOutputStream stream = null;
					try {
						stream = new FileOutputStream(fileInfo.getName());
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
                	file.download(stream);
                	
                	//Input stream for the file in local file system to be written to HDFS
                	InputStream in = null;
					try {
						in = new BufferedInputStream(new FileInputStream(fileInfo.getName()));
					} catch (FileNotFoundException e1) {
						e1.printStackTrace();
					}
                	
                	try{
//                        Path pt=new Path("hdfs://tspanndev10.field.hortonworks.com:8020/box/" + fileInfo.getName());
                        
                        System.out.println("Save to HDFS " + fileInfo.getName());
                        
                     
                       
//                        FileSystem fs = FileSystem.get(new Configuration());
//                        file.download(fs.create(pt,true));
//                        fs.close();
                        
                      //Destination file in HDFS
                        Configuration conf = new Configuration();
                        System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
                        String dst = "hdfs://tspanndev10.field.hortonworks.com:8020/box/" + fileInfo.getName();
                        
                        FileSystem fs = FileSystem.get(URI.create(dst), conf);
                        OutputStream out = fs.create(new Path(dst));
                       
                      //Copy file from local to HDFS
                      IOUtils.copyBytes(in, out, 4096, true);
                     
                      java.nio.file.Path path = FileSystems.getDefault().getPath(fileInfo.getName());                     
                      Files.delete(path);
                      
//                        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
//                                                   // TO append data to a file, use fs.append(Path f)
//                        String line;
//                        line="This file was created by a java program.";
//                        System.out.println(line);
//                        br.write(line);
//                        br.close();
        	        }catch(Exception e){
        	        	e.printStackTrace();
        	            System.out.println("File not found");
        	        }
                	
//                	try {
//						stream.close();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
                }               
            } 
        }        
    }

    private static void listFolder(BoxFolder folder, int depth) {
        for (BoxItem.Info itemInfo : folder) {
            String indent = "";
            for (int i = 0; i < depth; i++) {
                indent += "    ";
            }

            // you need this ID for accessing a folder
            System.out.println(indent + itemInfo.getName() + ",ID=" + itemInfo.getID() );
            
            if (itemInfo instanceof BoxFolder.Info) {
                BoxFolder childFolder = (BoxFolder) itemInfo.getResource();
                if (depth < MAX_DEPTH) {
                    listFolder(childFolder, depth + 1);
                }
            }
        }
    }
}
