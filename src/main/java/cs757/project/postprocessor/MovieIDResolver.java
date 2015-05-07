package cs757.project.postprocessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MovieIDResolver {
	
	private static Map<String,String> movieMap = new HashMap<String,String>(); 
	
    public static void main(String[] args) throws IOException{

		movieMap = buildMovieMap(); 
		List<String> clusters = Files.readAllLines(FileSystems.getDefault().getPath(".",args[0]), StandardCharsets.UTF_8); 
		int i = 1; 
		for (String cluster: clusters) {
		System.out.println("Cluster "  + i); 
		String centroid = cluster.split("\t")[0]; 
		if ("unclustered".equals(centroid)) {
			System.exit(0); 

		}
		String [] movieRatingPairs = centroid.split(","); 
			for (String movieRatingPair : movieRatingPairs) {

				String movieID = movieRatingPair.split(":")[0]; 
				String rating  = movieRatingPair.split(":")[1]; 
				String movieName = movieMap.get(movieID); 
				System.out.println(movieName + "\t\t\t" + rating); 

			} 
		i = i +1;  

		}
	}//main


	public static Map<String,String> buildMovieMap() throws IOException {
		Map<String,String> retv = new HashMap<String,String>(); 

		List<String> lines = Files.readAllLines(FileSystems.getDefault().getPath(".","movies.dat"), StandardCharsets.ISO_8859_1); 
		for (String line : lines) {
		String movieID = line.split("::")[0]; 
		String movieName  = line.split("::")[1]; 
		retv.put(movieID, movieName); 
		}

	return retv; 


	}
		
		
}//class
