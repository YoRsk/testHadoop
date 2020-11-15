package shiyan7;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomTxtFileCreator {

	public static void main(String[] args) {
		long start=System.currentTimeMillis();
		int numOfFiles = 100;
		int numOfRecorders = 100000;
		//本地文件位置，修改合適的位置
		String uri = "src/main/java/randomFilesPlus";
		FileOutputStream fout = null;
		Random ra = new Random();
		try {
			for (int i = 1; i <= numOfFiles; i++) {
				System.out.println("writing file#"+i);
				fout = new FileOutputStream(new File(uri + "/file" + i));
				List<String> list = new ArrayList<String>();
				for (int j = 0; j < numOfRecorders; j++)
					list.add(ra.nextInt(numOfRecorders) + 1 + "\t" + "the recorder #" + j + " in file#" + i);
				PrintStream pStream = new PrintStream(new BufferedOutputStream(fout));
				for (String str : list) {
					pStream.println(str);
				}
				pStream.close();
				fout.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {

		}
		long end=System.currentTimeMillis();
		System.out.println("write "+numOfFiles+" files successfully in "+ (end-start)+"ms");

	}

}
