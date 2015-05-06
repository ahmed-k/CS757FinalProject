package cs757.project.preprocessor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;

public class RemoveRareMovies {
	
	private static final String SEPARATOR = "\\s";
	
    public static void main(String[] args) throws IOException{
        
    	ClassLoader classLoader = RemoveRareMovies.class.getClassLoader();
		File file = new File(classLoader.getResource("datasets100K/u.data").getFile());
		
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
		
		int percentile = 35;
		int cutoff = ordered.get((ordered.size()*percentile)/100);
		double ratingsOnRareMovies = 0.0;
		TreeSet<Integer> reducedMovies = new TreeSet<Integer>();
		TreeSet<Integer> reducedUsers = new TreeSet<Integer>();
		
		scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
		 	String tokens[] = line.split(SEPARATOR);
		 	String movieId = tokens[1];
		 	Integer ratingsCount = map.get(movieId);
		 	if ( ratingsCount <= cutoff )
		 		ratingsOnRareMovies++;
		 	else {
		 		reducedMovies.add(Integer.valueOf(movieId));
		 		reducedUsers.add(Integer.valueOf(tokens[0]));
		 	}
		}
		scanner.close();
		System.out.println("By removing bottom "+percentile+" percentile of movies, M loses "+(100*ratingsOnRareMovies)/numOfRatings + " percent of ratings");
		
		TreeMap<Integer, Integer> movieIdMap = new TreeMap<Integer, Integer>();
		TreeMap<Integer, Integer> userIdMap = new TreeMap<Integer, Integer>();
		Integer newMovieId = 1;
		Integer newUserId = 1;
		for ( Integer movieId : reducedMovies )
			movieIdMap.put(movieId, newMovieId++);
		for ( Integer userId : reducedUsers )
			userIdMap.put(userId, newUserId++);
		
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
		 		Integer updatedMovieId = movieIdMap.get(Integer.valueOf(movieId));
		 		Integer updatedUserId = userIdMap.get(Integer.valueOf(tokens[0]));
		 		bw.write(updatedUserId+"::"+updatedMovieId+"::"+tokens[2]);
			 	bw.newLine();
			}
		}
		scanner.close();
		bw.close();
		
		File movieMapping = new File("movie_id_map.txt");
		if (!movieMapping .exists()) 
			movieMapping .createNewFile();
		fw = new FileWriter(movieMapping.getAbsoluteFile());
		bw = new BufferedWriter(fw);
		for ( Integer oldId : movieIdMap.keySet() )
			bw.write(oldId+"::"+movieIdMap.get(oldId)+"\n");
	 	scanner.close();
		bw.close();
		
		File userMapping = new File("user_id_map.txt");
		if (!userMapping .exists()) 
			userMapping .createNewFile();
		fw = new FileWriter(userMapping.getAbsoluteFile());
		bw = new BufferedWriter(fw);
		for ( Integer oldId : userIdMap.keySet() )
			bw.write(oldId+"::"+userIdMap.get(oldId)+"\n");
	 	scanner.close();
		bw.close();
		
    }
}
