
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	private Text left;
	private Text right;
	private String delimiter = ",";

	public TextPair() {
		left = new Text();
		right =  new Text();
	}

	public void set(String L, String R) {
		this.left.set(L);
		this.right.set(R);
	}

	public Text getLeft() {
		return left;
	}
	public void setLeft(String L) {
		this.left.set(L);
	}
	public Text getRight() {
		return right;
	}
	public void setRight(String R) {
		this.right.set(R);
	}

	
	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);

	}
	
	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);

	} 
	
	public int compareTo(TextPair a) {
		int compare = left.compareTo(a.getLeft());
		if (compare == 0) {
			compare = right.compareTo(a.getRight());
		}
		return compare;
	}

	public int baseCompareTo(TextPair b) {
		int compare = left.compareTo(b.getLeft());
		return compare;
	}

	public int hashCode() {
		return left.hashCode() * 31 + right.hashCode();
	}

	public int baseHashCode() {
		return Math.abs(left.hashCode());
	}

	public boolean equals(Object obj) {
		boolean isEqual =  false;
		if (obj instanceof TextPair) {
			TextPair isPair = (TextPair)obj;
			isEqual = left.equals(isPair.left) && right.equals(isPair.right);
		}

		return isEqual;
	}

	public void setDelim(String delimiter) {
		this.delimiter = delimiter;
	}

	public String toString() {
		return  left + delimiter + right +" ";
	}

}