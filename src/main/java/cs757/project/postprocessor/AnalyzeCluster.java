package cs757.project.postprocessor;

import cs757.project.preprocessor.RemoveRareMovies;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class AnalyzeCluster {
	
	private static final String SEPARATOR = "\\s";
	
    public static void main(String[] args) throws IOException{
        
    	ClassLoader classLoader = RemoveRareMovies.class.getClassLoader();
    	
    	File file = new File(classLoader.getResource("1M/users.txt").getFile());
		
    	Map<String,UserAttr> map = new HashMap<String,UserAttr>();
    	
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] tokens = line.split("::");
			map.put(tokens[0], new UserAttr(tokens[1], tokens[2], tokens[3]));
		}
		scanner.close();
    	
    	file = new File(classLoader.getResource("1M/step2_output.txt").getFile());
		List<List<UserAttr>> clusters = new ArrayList<List<UserAttr>>();
		List<UserAttr> everyone = new ArrayList<UserAttr>();
		clusters.add(everyone);
		
		scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if ( line.startsWith("unclustered") )
				continue;
			String usersLine = line.split("\\s")[1];
			String users[] = usersLine.split(",");
			List<UserAttr> cluster = new ArrayList<UserAttr>();
			for ( String u : users ){
				cluster.add(map.get(u));
				everyone.add(map.get(u));
			}
			clusters.add(cluster);
		}
		scanner.close();
		
		
		int allMales = 0;
		int[] allAge = new int[57];
		int[] allOccupation = new int[21];
		Arrays.fill(allAge, 0);
		Arrays.fill(allOccupation, 0);
		
		double allTotal = 0.0;
		for ( UserAttr user : everyone ){
			if ( user.gender.equals("M") )
				allMales++;
			allAge[user.age]++;
			
			if ( user.occupuation != 0 )
				allOccupation[user.occupuation]++;
			allTotal++;
		}
		
		int k = 1;
		for ( int j = 1; j < clusters.size(); j++ ){
			
			List<UserAttr> cluster = clusters.get(j);
			
			int males = 0;
			int[] age = new int[57];
			int[] occupation = new int[21];
			Arrays.fill(age, 0);
			Arrays.fill(occupation, 0);
			
			double total = 0.0;
			for ( UserAttr user : cluster ){
				if ( user.gender.equals("M") )
					males++;
				age[user.age]++;
				
				if ( user.occupuation != 0 )
					occupation[user.occupuation]++;
				total++;
			}
			
			System.out.println("cluster "+ (k++) );
			int i = 1;
			
//			System.out.println("male "+males/(double)total); 
//			  System.out.println("academic/educator "+occupation[i++]/(double)total);
//			  System.out.println("artist "+occupation[i++]/(double)total);
//			  System.out.println("clerical/admin "+occupation[i++]/(double)total);
//			  System.out.println("college/grad student "+occupation[i++]/(double)total);
//			  System.out.println("customer service "+occupation[i++]/(double)total);
//			  System.out.println("doctor/health care "+occupation[i++]/(double)total);
//			  System.out.println("executive/managerial "+occupation[i++]/(double)total);
//			  System.out.println("farmer "+occupation[i++]/(double)total);
//			  System.out.println("homemaker "+occupation[i++]/(double)total);
//			 System.out.println("K-12 student "+occupation[i++]/(double)total);
//			 System.out.println("lawyer "+occupation[i++]/(double)total);
//			 System.out.println("programmer "+occupation[i++]/(double)total);
//			 System.out.println("retired "+occupation[i++]/(double)total);
//			 System.out.println("sales/marketing "+occupation[i++]/(double)total);
//			 System.out.println("scientist "+occupation[i++]/(double)total);
//			 System.out.println("self-employed "+occupation[i++]/(double)total);
//			 System.out.println("technician/engineer "+occupation[i++]/(double)total);
//			 System.out.println("tradesman/craftsman "+occupation[i++]/(double)total);
//			 System.out.println("unemployed "+occupation[i++]/(double)total);
//			 System.out.println("writer "+occupation[i++]/(double)total);
//			 System.out.println("");
			
//			 1:  "Under 18"
//			* 18:  "18-24"
//			* 25:  "25-34"
//			* 35:  "35-44"
//			* 45:  "45-49"
//			* 50:  "50-55"
//			* 56
			
			System.out.println("Under 18 "+age[1]/(double)total);
			System.out.println("18-24 "+age[18]/(double)total);
			System.out.println("25-34 "+age[25]/(double)total);
			System.out.println("35-44 "+age[35]/(double)total);
			System.out.println("45-49 "+age[45]/(double)total);
			System.out.println("50-55 "+age[50]/(double)total);
			System.out.println("56    "+age[56]/(double)total);
			
			
			System.out.println("");
			
		}
	
		System.out.println("everyone");
		
//		int i=1;
//		System.out.println("male "+allMales/(double)allTotal); 
//		  System.out.println("academic/educator "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("artist "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("clerical/admin "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("college/grad student "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("customer service "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("doctor/health care "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("executive/managerial "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("farmer "+allOccupation[i++]/(double)allTotal);
//		  System.out.println("homemaker "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("K-12 student "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("lawyer "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("programmer "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("retired "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("sales/marketing "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("scientist "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("self-employed "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("technician/engineer "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("tradesman/craftsman "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("unemployed "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("writer "+allOccupation[i++]/(double)allTotal);
//		 System.out.println("");
		
		System.out.println("Under 18 "+allAge[1]/(double)allTotal);
		System.out.println("18-24 "+allAge[18]/(double)allTotal);
		System.out.println("25-34 "+allAge[25]/(double)allTotal);
		System.out.println("35-44 "+allAge[35]/(double)allTotal);
		System.out.println("45-49 "+allAge[45]/(double)allTotal);
		System.out.println("50-55 "+allAge[50]/(double)allTotal);
		System.out.println("56    "+allAge[56]/(double)allTotal);

				
		System.out.println("ok");
    }
}
