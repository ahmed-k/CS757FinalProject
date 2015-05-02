package cs757.project.preprocessor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;


public class RemoveRareMovies {
	
    public static void main(String[] args) throws IOException{
        
    	ClassLoader classLoader = RemoveRareMovies.class.getClassLoader();
		File file = new File(classLoader.getResource("10M/updated_ratings.txt").getFile());
		
		HashMap<String, Integer> map = new HashMap<String,Integer>();
		double ratings = 0;
		
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			ratings++;
			String line = scanner.nextLine();
		 	String movieId = line.split("::")[1];
		 	Integer prev = map.put(movieId, 1);
		 	if ( prev != null )
		 		map.put(movieId, prev+1);
		}
		scanner.close();
		
		Collection<Integer> counts = map.values();
		ArrayList<Integer> ordered = new ArrayList<Integer>(counts);
		Collections.sort(ordered);
		
		System.out.println("size="+ordered.size());
		int increments = ordered.size()/20;
		
		int cutoff = 0;
		int percentile = 35;
		for ( int i = 1; i < 20; i++ ){
			System.out.println("percentile="+i*5+", counts="+ordered.get(i*increments));
			if ( i*5 == percentile )
				cutoff = ordered.get(i*increments);
		}
		
		double ratingsOnRareMovies = 0.0;
		scanner = new Scanner(file);
		File outputFile = new File("reduced_ratings.txt");
		if (!outputFile.exists()) 
			outputFile.createNewFile();
		FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
		 	String movieId = line.split("::")[1];
		 	Integer numOfRatings = map.get(movieId);
		 	if ( numOfRatings <= cutoff )
		 		ratingsOnRareMovies++;
		 	else {
		 		bw.write(line);
			 	bw.newLine();
		 	}
		}
		scanner.close();
		bw.close();
		
		System.out.println("By removing bottom "+percentile+" percentile of movies, M loses "+(100*ratingsOnRareMovies)/ratings + " percent of ratings");
    }
}
