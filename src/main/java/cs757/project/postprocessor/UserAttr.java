package cs757.project.postprocessor;

public class UserAttr {
	protected final int age, occupuation;
	protected final String gender;
	public UserAttr(String gender, String age, String occupation){
		this.gender = gender;
		this.age = Integer.valueOf(age);
		this.occupuation = Integer.valueOf(occupation);
	}
}