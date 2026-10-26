package radar.udf;

import java.time.Instant;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Milisegundos desde 1970 de un TIMESTAMP_LTZ. FIRST_VALUE en ventanas OVER no acepta
 * TIMESTAMP_LTZ, así que el instante anterior se guarda como número y se reconstruye con
 * TO_TIMESTAMP_LTZ(ms, 3).
 */
public class EpochMillis extends ScalarFunction {
    public Long eval(Instant ts) {
        return ts == null ? null : ts.toEpochMilli();
    }
}
