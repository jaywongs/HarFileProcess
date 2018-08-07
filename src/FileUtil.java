import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class FileUtil {
	
	public static String getJarDir() {
		String path = FileUtil.class.getProtectionDomain().getCodeSource().getLocation().getFile();
		try {
			path = java.net.URLDecoder.decode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new File(path).getParent();
	}
	
	public static Properties getConfig(String filename) throws IOException {
		File file = new File(getJarDir() + "/" + filename);
		InputStream ins = new FileInputStream(file);
		Properties ps = new Properties();
		ps.load(ins);
		return ps;
	}
	public static Properties getConfigByPath(String filename) throws IOException {
		File file = new File(filename);
		InputStream ins = new FileInputStream(file);
		Properties ps = new Properties();
		ps.load(ins);
		return ps;
	}
	
	public static byte[] read(String file) {
		try {
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			byte buf[] = new byte[in.available()];
			in.read(buf, 0, buf.length);
			in.close();
			return buf;
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	public static boolean write(String file, byte[] data, boolean append) {
		try {
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, append));
			out.write(data);
			out.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
}
