package cs757.project.clustering;

public class CentroidPair {
	
	final Centroid c1;
	final Centroid c2;
	final String id;
	
	public CentroidPair(Centroid c1, Centroid c2){
		this.c1 = c1;
		this.c2 = c2;
		this.id = c1.hashCode()+""+c2.hashCode();
	}
	

}
