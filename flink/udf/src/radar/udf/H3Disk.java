package radar.udf;

import com.uber.h3core.H3Core;
import java.io.IOException;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * La celda H3 de una posición y sus vecinas a distancia k, una fila por celda. Sirve para juntar
 * barcos cercanos aunque caigan a ambos lados del borde de una celda.
 */
@FunctionHint(output = @DataTypeHint("ROW<cell STRING>"))
public class H3Disk extends TableFunction<Row> {
    private transient H3Core h3;

    @Override
    public void open(FunctionContext context) throws IOException {
        h3 = H3Core.newInstance();
    }

    public void eval(Double lat, Double lon, Integer res, Integer k) {
        for (String cell : cells(lat, lon, res, k)) collect(Row.of(cell));
    }

    java.util.List<String> cells(Double lat, Double lon, Integer res, Integer k) {
        if (lat == null || lon == null || res == null || k == null) return java.util.List.of();
        if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return java.util.List.of();
        if (h3 == null) {
            try {
                h3 = H3Core.newInstance();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return h3.gridDisk(h3.latLngToCellAddress(lat, lon, res), k);
    }
}
