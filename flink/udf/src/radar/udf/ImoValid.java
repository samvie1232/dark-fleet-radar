package radar.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Valida un número IMO de 7 dígitos con su dígito de control: los 6 primeros dígitos se
 * multiplican por 7, 6, 5, 4, 3 y 2; el último dígito de la suma debe ser el séptimo.
 */
public class ImoValid extends ScalarFunction {
    public static boolean isValid(long imo) {
        if (imo < 1_000_000L || imo > 9_999_999L) return false;
        String s = Long.toString(imo);
        int sum = 0;
        for (int i = 0; i < 6; i++) sum += (s.charAt(i) - '0') * (7 - i);
        return sum % 10 == s.charAt(6) - '0';
    }

    public Boolean eval(Long imo) {
        return imo != null && isValid(imo);
    }
}
