package radar.udf;

import org.apache.flink.table.functions.ScalarFunction;

/** Velocidad necesaria (nudos) para recorrer `distKm` en `seconds`. NULL si el intervalo no es positivo. */
public class ImpliedSpeedKnots extends ScalarFunction {
    static final double KM_PER_NAUTICAL_MILE = 1.852;

    public static Double speed(double distKm, double seconds) {
        if (seconds <= 0) return null;
        return (distKm / KM_PER_NAUTICAL_MILE) / (seconds / 3600.0);
    }

    public Double eval(Double distKm, Long seconds) {
        if (distKm == null || seconds == null) return null;
        return speed(distKm, seconds);
    }
}
