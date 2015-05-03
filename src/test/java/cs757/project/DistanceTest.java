package cs757.project;

import java.util.Map;

import cs757.project.clustering.Distance;
import junit.framework.Assert;

import org.junit.Test;


public class DistanceTest {
	
	
	private String 	A = "1:4,4:5,5:1,",
					B = "1:5,2:5,3:4",
					C = "4:2,5:4,6:5,";
	
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
	
    

}
