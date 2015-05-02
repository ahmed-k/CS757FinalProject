package cs757.project;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;


public class DistanceTest {
	
	
	private String 	A = "4,,,5,1,,",
					B = "5,5,4,,,,",
					C = ",,,2,4,5,";
	
	private String[] vectorA, vectorB, vectorC;
	
	@Before
	public void init(){
		vectorA = A.split(",", -1);
		vectorB = B.split(",", -1);
		vectorC = C.split(",", -1);
	}
	
	@Test
	public void jaccardBag(){
		System.out.println(Distance.jaccardBag(vectorA, vectorB));
		System.out.println(Distance.jaccardBag(vectorA, vectorC));
		System.out.println(Distance.jaccardBag(vectorB, vectorC));
	}
	
//	@Test
//	public void converVectorToMap(){
//		Map<Integer, String> map = Distance.convertToMap(vectorA);
//		Assert.assertEquals("4", map.get(1));
//		Assert.assertEquals(null, map.get(2));
//		Assert.assertEquals(null, map.get(3));
//		Assert.assertEquals("5", map.get(4));
//		Assert.assertEquals("1", map.get(5));
//	}
	
    

}
