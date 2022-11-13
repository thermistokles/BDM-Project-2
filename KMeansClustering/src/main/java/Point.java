package kmeans;

public class Point {
    private double x, y;

    public Point(int x, int y) {
        this.x = x;
        this.y = y;
    }

    Point(String x, String y) {
        this.x = Double.parseDouble(x);
        this.y = Double.parseDouble(y);
    }

    public Point(String point) {
        System.out.println("point: "+ point);
        String[] vals = point.split(",");
        this.x = Double.parseDouble((vals[0]));
        this.y = Double.parseDouble((vals[1]));
    }

    public Point addPoint(Point point) {
        this.x += point.x;
        this.y += point.y;
        return this;
    }

    public Point divide(double s) {
        this.x = this.x / s;
        this.y = this.y / s;
        return this;
    }

    public double[] getVector() {
        return new double[]{x, y};
    }

    @Override
    public String toString() {
        return x + "," + y;
    }
}