package etcClasses;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import dataStructureClasses.Point;

public class DataGenerator {

	
	public List<String> singleWord;
	
	
	public DataGenerator(){
		singleWord = new ArrayList<>();
		singleWord.add("a");
		singleWord.add("b");
		singleWord.add("c");
		singleWord.add("d");
		singleWord.add("e");
		singleWord.add("f");
		singleWord.add("g");
		singleWord.add("h");
		singleWord.add("i");
		singleWord.add("j");
		singleWord.add("k");
		singleWord.add("o");
		singleWord.add("p");
		singleWord.add("x");
		singleWord.add("y");
		singleWord.add("z");
		
	}
	
	public static void main(String[] args) throws IOException {
		String path = "/Users/haozhouwang/Dropbox/MyUQ/project data/hadoop/3.txt";
		DataGenerator dg = new DataGenerator();
		File file = new File(path);
		BufferedWriter out = new BufferedWriter
				(new OutputStreamWriter
						(new FileOutputStream(file,true), "utf-8"));
		
		int numberOfPoint = 9000;
		
		for(int i = 0; i < numberOfPoint; i++){
			out.write(dg.getRandomPoint(i).toString() + "#" + dg.getRandomKeyWord());
			out.newLine();
		}
		out.flush();
		out.close();
		
		
		

		
		
		

	}
	
	public Point getRandomPoint(int id){
		
		Float x = 116 + ((float) Math.random());
		Float y = 38 + ((float) Math.random());
		
		return new Point(id, x, y);
		
		
	}
	
	public String getRandomKeyWord(){
		Double seed = Math.random();
		seed = seed * 10;
		int numberOfWords = seed.intValue() + 1;
		String words = "";
		
		for(int i = 0; i < numberOfWords; i++){
			words = words + singleWord.get(i) +  ",";
		}
		words = words + "t";
		
		return words;
	}

}
