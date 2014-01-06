package jp.opensquare.mahout.clustering.sample;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MeigenScraping {
	static final String TARGET_URL = "http://simple.s59.xrea.com/all_word/all_word.php/all";

	public static void main(String[] args) throws Exception {
		String name = "〃";
		Document doc = Jsoup.connect(TARGET_URL).get();
		Elements contents = doc.select("form .list");
		for (Element row : contents) {
			Elements tds = row.select("td");
			for (int i = 0; i < tds.size(); i++) {
				if (i > 2) {
					try {
						Integer.parseInt(tds.get(i - 2).text());
						// get name
						if(!tds.get(i - 1).text().equals("〃")) {
							name = tds.get(i - 1).text();
						}
						String meigen = tds.get(i).text().replaceAll("ガンダム", "gundam");
						meigen = meigen.replaceAll("・", "");
						meigen = meigen.replaceAll("！", "");
						meigen = meigen.replaceAll("？", "");
						meigen = meigen.replaceAll("･", "");
						if(name.trim().length() > 0) {
							System.out.println(name + " : " + meigen);
							write(name, meigen);
						}
					} catch (Exception e) {
					}
				}
			}
		}
	}
	static int fileNo = 1;
	private static void write(String name, String meigen) {
		File path = new File("meigen");
		File file = new File(path, "名言-" + name + "-" + String.format("%05d", fileNo++) + ".txt");
		if(!path.exists()) {
			path.mkdir();
		}

		Writer writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));
			writer.write(meigen);
			writer.write("\n");
			writer.close();
		} catch (IOException e) {
			System.out.println(e);
		}
	}
}
