package radar.udf;

import org.apache.flink.table.functions.ScalarFunction;

/** Distancia ortodrómica en km entre dos posiciones. */
public class HaversineKm extends ScalarFunction {
    static final double EARTH_RADIUS_KM = 6371.0088;

    public static double distance(double lat1, double lon1, double lat2, double lon2) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double dp = p2 - p1;
        double dl = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dp / 2) * Math.sin(dp / 2)
                + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    public Double eval(Double lat1, Double lon1, Double lat2, Double lon2) {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) return null;
        return distance(lat1, lon1, lat2, lon2);
    }
}
