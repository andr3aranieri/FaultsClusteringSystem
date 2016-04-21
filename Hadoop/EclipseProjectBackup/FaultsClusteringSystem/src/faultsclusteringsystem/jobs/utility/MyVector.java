package faultsclusteringsystem.jobs.utility;

import java.util.Arrays;
import java.util.Locale;

public class MyVector {
	
	private Double[] vector;
	private String name;
	private String separator;
	
	public MyVector(String sVector, String n) {
		this(sVector, n, "-");
	}

	public MyVector(String sVector, String n, String sep) {
		this.vector = this.getVector(sVector);
		this.name = n;
		this.separator = sep;
	}
	
	public MyVector(Double[] v, String n) {
		this.vector = v;
		this.name = n;
		this.separator = "-";
	}

	public MyVector(String n, int dim) {
		this.vector = createEmptyArray(dim);
		this.name = n;
		this.separator = "-";
	}
	
	private Double[] createEmptyArray(int dim) {
		Double[] ret = new Double[dim];
		for(int i = 0; i < dim; i++) {
			ret[i] = 0.0;
		}
		
		return ret;
	}
	
	public Double[] getInnerVector() {
		return this.vector;
	}

	private Double[] getVector(String s) {
		String[] aValues = s.split("-");
		Double[] vector = new Double[aValues.length];
		for(int i = 0; i < aValues.length; i++) {
			vector[i] = Double.parseDouble(aValues[i]);
		}		
		return vector;
	}
	
	public String getName() {
		return this.name;
	}
	
	public double cosineDistance(MyVector v2) {
		double pr = 0.0;
		double mod1 = 0.0;
		double mod2 = 0.0;
		for (int i = 0; i < this.vector.length; i++) {
			pr += this.vector[i] * v2.vector[i];
			mod1 += Math.pow(this.vector[i], 2);
			mod2 += Math.pow(v2.vector[i], 2);
		}
				
		return pr / (Math.sqrt(mod1) * Math.sqrt(mod2));
	}
	
	public double euclideanDistance(MyVector v2) {
		double pr = 0.0;
		for (int i = 0; i < this.vector.length; i++) {
			pr +=  Math.pow((this.vector[i] - v2.vector[i]), 2);
		}
		return Math.sqrt(pr);
	}
	
	public double manhattanDistance(MyVector v2) {
		double manhattanDistance = 0.0;
		for (int i = 0; i < this.vector.length; i++) {
			manhattanDistance +=  Math.abs(this.vector[i] - v2.vector[i]);
		}
		return manhattanDistance;		
	}

	public MyVector mySum(MyVector v2) {
		MyVector sum = new MyVector(this.getName(), this.vector.length);
		for(int i = 0; i < this.vector.length; i++) {
			sum.vector[i] = this.vector[i] + v2.vector[i];
		}
		return sum;
	}
	
	@Override
	public boolean equals(Object obj) {
		return Arrays.equals(this.vector, ((MyVector) obj).vector);
	}
	
	@Override
	public String toString() {
		String ret = "";
		for (double d: this.vector) {
			ret += String.format(Locale.US, "%.6f", d) + this.separator;
		}
		ret = ret.substring(0, ret.length()-1);
		return ret;
	}
}

