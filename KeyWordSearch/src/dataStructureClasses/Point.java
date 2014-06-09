package dataStructureClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class Point implements WritableComparable{

	private int pointId;
	private float x;
	private float y;
	
	public Point(){
		
	}
	
	public Point(int id, float x, float y){
		this.pointId = id;
		this.x = x;
		this.y = y;
	}
	
	public int getPointId() {
		return pointId;
	}

	public void setPointId(int pointId) {
		this.pointId = pointId;
	}

	public float getX() {
		return x;
	}

	public void setX(float x) {
		this.x = x;
	}

	public float getY() {
		return y;
	}

	public void setY(float y) {
		this.y = y;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.pointId = in.readInt();
		this.x = in.readFloat();
		this.y = in.readFloat();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.pointId);
		out.writeFloat(this.x);
		out.writeFloat(this.y);
		
	}
	
	public String toString(){
		return this.pointId + "," + this.x + "," + this.y;
	}

	@Override
	public int compareTo(Object o) {
		return Integer.compare(this.pointId, ((Point) o).getPointId());
	}

}
