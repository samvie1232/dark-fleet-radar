package radar.udf;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Clave para comparar nombres de barco: mayúsculas, solo letras y números, y las palabras en
 * orden alfabético. "VLI-25 CINDY" y "Cindy  VLI 25" dan la misma clave ("25 CINDY VLI"), así un
 * barco que escribe su nombre de dos formas no parece cambiar de identidad.
 */
public class NameKey extends ScalarFunction {
    public static String key(String name) {
        if (name == null) return null;
        String k = Arrays.stream(name.toUpperCase().split("[^\\p{L}\\p{N}]+"))
                .filter(w -> !w.isEmpty())
                .sorted()
                .collect(Collectors.joining(" "));
        return k.isEmpty() ? null : k;
    }

    public String eval(String name) {
        return key(name);
    }
}
