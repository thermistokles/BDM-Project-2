import kmeans.Point;

public class Distance {

    static double getDistance(Point p1, Point p2) throws Exception {
        double[] p1Vector = p1.getVector();
        double[] p2Vector = p2.getVector();

        double sum = 0f;
        for (int i = 0; i < p1Vector.length; i++) {
            sum += Math.pow(p1Vector[i] - p2Vector[i], 2);
        }
        return Math.sqrt(sum);

    }
}
