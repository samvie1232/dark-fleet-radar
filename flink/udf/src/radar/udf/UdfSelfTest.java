package radar.udf;

/** Pruebas de las UDF. Se ejecutan al construir la imagen de Flink: si fallan, falla el build. */
public class UdfSelfTest {
    private static int failures = 0;

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "OK    " : "FALLA ") + name);
        if (!ok) failures++;
    }

    private static boolean near(double a, double b, double tol) {
        return Math.abs(a - b) <= tol;
    }

    public static void main(String[] args) throws Exception {
        // Haversine: 1 grado de latitud ~ 111,195 km; mismo punto = 0.
        check("haversine 1 grado de latitud", near(HaversineKm.distance(55, 12, 56, 12), 111.195, 0.01));
        check("haversine mismo punto", HaversineKm.distance(55.5, 12.3, 55.5, 12.3) == 0.0);
        check("haversine nulos", new HaversineKm().eval(null, 1.0, 2.0, 3.0) == null);

        // Velocidad implícita: 1,852 km en 1 h = 1 nudo; intervalo 0 o negativo = NULL.
        check("velocidad 1 nudo", near(ImpliedSpeedKnots.speed(1.852, 3600), 1.0, 1e-9));
        check("velocidad 100 km en 30 min = 108 nudos", near(ImpliedSpeedKnots.speed(100, 1800), 107.99, 0.01));
        check("velocidad intervalo 0", ImpliedSpeedKnots.speed(5, 0) == null);
        check("velocidad intervalo negativo", ImpliedSpeedKnots.speed(5, -10) == null);

        // IMO: 9074729 es válido (ejemplo clásico); cambiar el dígito de control lo invalida.
        check("IMO válido 9074729", ImoValid.isValid(9074729L));
        check("IMO con control erróneo", !ImoValid.isValid(9074728L));
        check("IMO cero", !ImoValid.isValid(0L));
        check("IMO de 8 dígitos", !ImoValid.isValid(90747290L));
        check("IMO nulo", !new ImoValid().eval(null));

        // H3: ejemplo de la documentación oficial (San Francisco, resolución 9).
        H3Cell h3 = new H3Cell();
        check("H3 ejemplo oficial", "8928308280fffff".equals(h3.eval(37.775938728915946, -122.41795063018799, 9)));
        check("H3 posición no disponible (91/181)", h3.eval(91.0, 181.0, 7) == null);
        check("H3 nulos", h3.eval(null, 12.0, 7) == null);

        // EpochMillis
        check("epoch millis", new EpochMillis().eval(java.time.Instant.parse("2026-10-26T15:00:00.123Z")) == 1793026800123L);
        check("epoch millis nulo", new EpochMillis().eval(null) == null);

        if (failures > 0) {
            System.out.println(failures + " pruebas fallaron");
            System.exit(1);
        }
        System.out.println("Todas las pruebas de UDF pasaron");
    }
}
