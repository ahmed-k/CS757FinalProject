package cs757.project;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeSet;

public class RemoveRareMovies {
	
	private static final String SEPARATOR = "::";
	
    public static void main(String[] args) throws IOException{
        
    	ClassLoader classLoader = RemoveRareMovies.class.getClassLoader();
		File file = new File(classLoader.getResource("10M/ratings.dat").getFile());
		
		HashMap<String, Integer> map = new HashMap<String,Integer>();
		double numOfRatings = 0;
		Scanner scanner = new Scanner(file);
		
		while (scanner.hasNextLine()) {
			numOfRatings++;
			String line = scanner.nextLine();
		 	String movieId = line.split(SEPARATOR)[1];
		 	Integer prev = map.put(movieId, 1);
		 	if ( prev != null )
		 		map.put(movieId, prev+1);
		}
		scanner.close();
		
		ArrayList<Integer> ordered = new ArrayList<Integer>(map.values());
		Collections.sort(ordered);
		
//		System.out.println("size="+ordered.size());
		int percentile = 35;
		int cutoff = ordered.get((ordered.size()*percentile)/100);
//		System.out.println("cutoff="+cutoff);
		
		double ratingsOnRareMovies = 0.0;
		TreeSet<Integer> reducedMovies = new TreeSet<Integer>();
		
		scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
		 	String movieId = line.split(SEPARATOR)[1];
		 	Integer ratingsCount = map.get(movieId);
		 	if ( ratingsCount <= cutoff )
		 		ratingsOnRareMovies++;
		 	else
		 		reducedMovies.add(Integer.valueOf(movieId));
		}
		scanner.close();
		System.out.println("By removing bottom "+percentile+" percentile of movies, M loses "+(100*ratingsOnRareMovies)/numOfRatings + " percent of ratings");
		
		HashMap<String, Integer> idMap = new HashMap<String, Integer>();
		Integer newId = 1;
		for ( Integer movieId : reducedMovies )
			idMap.put(String.valueOf(movieId), newId++);
		
		File outputFile = new File("reduced_ratings.txt");
		if (!outputFile.exists()) 
			outputFile.createNewFile();
		FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
		 	String tokens[]= line.split(SEPARATOR);
		 	String movieId = tokens[1];
		 	Integer ratingsCount = map.get(movieId);
		 	if ( ratingsCount > cutoff ) {
		 		Integer updatedId = idMap.get(movieId);
		 		bw.write(tokens[0]+"::"+updatedId+"::"+tokens[2]);
			 	bw.newLine();
			}
		}
		scanner.close();
		bw.close();
	
    }
}
