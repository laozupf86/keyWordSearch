package etcClasses;

import dataStructureClasses.Point;

public class DistanceCalculation {
	
	public static double calculateDistance(Point p1, Point p2){
		
		return Math.sqrt(Math.pow(p1.getX() - p2.getX(), 2) + Math.pow(p1.getY() - p2.getY(), 2));
		
	}

}
