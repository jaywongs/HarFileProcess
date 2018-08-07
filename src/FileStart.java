import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class FileStart {

    private static String HDFS_CONF_PATH;
    private static String HDFS_DIR;
    private static String PROV_PREFIX;
    private static String[] PreList;
    private static String HAR_DIR;

    public static Configuration conf;
    static {
        try {
            conf = new Configuration();
            Properties prop = FileUtil.getConfigByPath("./har.properties");
            HDFS_CONF_PATH = prop.getProperty("HDFS_CONF_PATH");
            HDFS_DIR = prop.getProperty("HDFS_DIR");
            PROV_PREFIX = prop.getProperty("PROV_PREFIX");
            PreList = PROV_PREFIX.split(",");
            HAR_DIR = prop.getProperty("HAR_DIR");
            if (HDFS_CONF_PATH.isEmpty()){
                System.err.println("Please set property \"HDFS_CONF_PATH\" at least");
                System.exit(1);
            }else {
                Path c = new org.apache.hadoop.fs.Path(HDFS_CONF_PATH+"hdfs-site.xml");
                Path h = new org.apache.hadoop.fs.Path(HDFS_CONF_PATH+"core-site.xml");
                conf.addResource(c);
                conf.addResource(h);
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        ScanHdfsDir scanHdfsDir = new ScanHdfsDir();

        Path testPath = null;
        testPath = new Path(HDFS_DIR);
        FileSystem fs = testPath.getFileSystem(conf);
        List<Path> provPaths = scanHdfsDir.getProvPath(fs, testPath, PreList);
        for (Path path:provPaths){
            System.out.println(path.toString());
        }

        ArchiveProcess archiveProcess = new ArchiveProcess();
        archiveProcess.archiveBySize(provPaths, null, HAR_DIR, conf);
    }
}
