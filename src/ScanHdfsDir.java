
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class ScanHdfsDir {
    private static String HDFS_CONF_PATH;
    private static String HDFS_DIR;
    private static String PROV_PREFIX;

    public static List<Path> getProvPath(FileSystem fs, Path folderPath, String[] preList)throws IOException{
        List<Path> paths = new ArrayList<>();
        List<Path> provPaths = new ArrayList<>();
        List<String> tmpList = Arrays.asList(preList);
        if (fs.exists(folderPath)){
            FileStatus[] fileStatuses = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatuses.length; i++){
                String pathList[] = fileStatuses[i].getPath().toString().split("/");
                if (fileStatuses[i].isDirectory() && tmpList.contains(pathList[pathList.length - 1])) {
                    provPaths.add(fileStatuses[i].getPath());
                    System.out.println(fileStatuses[i].getPath());
                }
            }
        }
        return provPaths;
    }

    public static List<Path> getFilePath(FileSystem fs, Path folderPath)throws IOException{
        List<Path> paths = new ArrayList<>();
        if (fs.exists(folderPath)){
            FileStatus[] fileStatuses = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatuses.length; i++){
                if (fileStatuses[i].isFile() && fileStatuses[i].getPath().getName().endsWith("txt.gz")){
                    paths.add(fileStatuses[i].getPath());
                    System.out.println(fileStatuses[i].getPath());
                }
            }
        }
        return paths;
    }

}
