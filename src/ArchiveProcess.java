import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ArchiveProcess {


//    public void archive(List<Path> provPaths, String archiveName, String HarDir, Configuration conf) throws Exception {
//
//        for (Path provPath:provPaths){
//
//            String tmpStrs[] = provPath.toString().split("/");
//            String provkey = tmpStrs[tmpStrs.length - 1];
//            String parentPathStr = provPath.toString();
//            Path archivePath = new Path(HarDir + provkey);
//            FileSystem fs = provPath.getFileSystem(conf);
//
//            RemoteIterator<LocatedFileStatus> fileList=fs.listFiles(provPath, true);
//            List<String> harNames = new ArrayList<>();
//            while (fileList.hasNext()){
//                LocatedFileStatus file = fileList.next();
//                Path filePath = file.getPath();
//                String fileName = filePath.getName();
//                if (file.isFile() && fileName.endsWith(".txt.gz")){
//                    String harName = provkey + fileName.substring(0,10); //截取到小时
//                    if (!harNames.contains(harName)){
//                        harNames.add(harName);
//                    }
//                }
//            }
//            for (String harNamepre:harNames){
//                if(!(fs.exists(archivePath))) { fs.mkdirs(archivePath); }
//                URI uri = fs.getUri();
//                String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
//                        + archivePath.toUri().getPath() + Path.SEPARATOR;
//                String harName = harNamepre + ".har";
//                String[] args = { "-archiveName", harName, "-p", parentPathStr, "*", archivePath.toString()};
//                HadoopArchives har = new HadoopArchives(conf);
//                ToolRunner.run(har, args);
//
////              fs.delete(new Path(parentPathStr),true);
//
//            }
//        }
//
//    }

    public void archiveBySize(List<Path> provPaths, String archiveName, String HarDir, Configuration conf) throws Exception {

        for (Path provPath:provPaths){

            String tmpStrs[] = provPath.toString().split("/");
            String provkey = tmpStrs[tmpStrs.length - 1];
            String parentPathStr = provPath.toString();
            Path archivePath = new Path(HarDir + provkey);
            FileSystem fs = provPath.getFileSystem(conf);

            RemoteIterator<LocatedFileStatus> fileList=fs.listFiles(provPath, true);
            List<String> harNames = new ArrayList<>();

            while (fileList.hasNext()){
                LocatedFileStatus file = fileList.next();
                Path filePath = file.getPath();
                String fileName = filePath.getName();
                if (file.isFile() && fileName.endsWith(".txt.gz")){
                    String harName = provkey + fileName.substring(0,10); //截取到小时
                    if (!harNames.contains(harName)){
                        harNames.add(harName);
                    }
                }
            }

            List<Path> filePath = ScanHdfsDir.getFilePath(fs, provPath);

            for (String harNamepre:harNames){
                if(!(fs.exists(archivePath))) { fs.mkdirs(archivePath); }
                URI uri = fs.getUri();
                String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
                        + archivePath.toUri().getPath() + Path.SEPARATOR;
                String harName = harNamepre + ".har";
                String[] args = { "-archiveName", harName, "-p", parentPathStr, "*", archivePath.toString()};
                HadoopArchives har = new HadoopArchives(conf);
                har.partSize = 32 * 1024 *1024l;
                har.blockSize = 16 *1024 *1024l;
                ToolRunner.run(har, args);

//              fs.delete(new Path(parentPathStr),true);

            }
        }

    }
}
