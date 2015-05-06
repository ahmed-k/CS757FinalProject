package cs757.project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import cs757.project.clustering.Centroid;
import cs757.project.clustering.Distance;
import cs757.project.clustering.Step1;


public class Tests {
	
	
	private String 	A = "1:4,4:5,5:1",
					B = "1:5,2:5,3:4",
					C = "4:2,5:4,6:5";
	
	@Test
	public void jaccardBag(){
		Assert.assertEquals(0.16, Distance.jaccardBag(A, B), 0.01);
		Assert.assertEquals(0.14, Distance.jaccardBag(A, C), 0.01);
		Assert.assertEquals(0.0, Distance.jaccardBag(B, C), 0.0);
	}
	
	@Test
	public void converVectorToMap(){
		Map<String, Integer> map = Distance.convertToMap(A);
		Assert.assertEquals(new Integer(4), map.get("1"));
		Assert.assertEquals(new Integer(5), map.get("4"));
		Assert.assertEquals(new Integer(1), map.get("5"));
	}
	
	@Test
	public void calcCentroid(){
		
		List<Map<String,Integer>> list = new ArrayList<Map<String,Integer>>(3);
		list.add(Distance.convertToMap(A));
		list.add(Distance.convertToMap(B));
		list.add(Distance.convertToMap(C));
		
		Map<String,Double>[] outputs = Step1.Step1Mapper.calcCentroid(list);
		
		Map<String,Double> centroid = outputs[0];
		Map<String,Double> weights = outputs[1];
		
//		for ( int i = 1; i <= 6; i ++ )
//			System.out.print(i+":"+centroid.get(i+"")+"\t");
//		System.out.println("");
//		for ( int i = 1; i <= 6; i ++ )
//			System.out.print(i+":"+weights.get(i+"")+"\t");
//		System.out.println("");
	}
	
	
	@Test
	public void mapToString(){
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("1", 0.5);
		map.put("2", 1.5);
		map.put("3", 2.5);
		System.out.println(Centroid.mapToString(map));
	}
	
	@Test
	public void cosine(){
		System.out.println(Distance.cosine(Centroid.convertToMap(A), Centroid.convertToMap(B)));
		System.out.println(Distance.cosine(Centroid.convertToMap(A), Centroid.convertToMap(C)));
	}
	
	@Test
	public void combineCentroid(){
		String centroid = "1:4.5,4:3.5,5:2.5";	
		String weights = "1:2.0,4:2.0,5:2.0";
		Centroid c1 = new Centroid(centroid, weights);
		Centroid c2 = new Centroid("2:1,3:3,5:5", "2:2,3:2,5:10");
		c1.combine(c2);
		System.out.println(c1);
	}
	
}
