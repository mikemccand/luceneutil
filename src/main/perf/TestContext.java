package perf;

import java.util.Objects;

/**
 * Test context enables using different parameters for baseline/candidate.
 * It can be handy when we want to run the same task against the same Lucene code,
 * but test two different implementations of a feature.
 */
public class TestContext {
    public final FacetMode facetMode;

    private TestContext(FacetMode facetMode) {
        this.facetMode = Objects.requireNonNull(facetMode);
    }

    public static TestContext parse(String context) {
        FacetMode facetMode = FacetMode.UNDEFINED;
        if (context.isEmpty() == false) {
            String[] contextParams = context.split(",");
            for (String param : contextParams) {
                String[] keyValue = param.split(":");
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Test context params must be key value pairs separated by colon, got: " + param);
                }
                if (keyValue[0].equals("facetMode")) {
                    facetMode = FacetMode.valueOf(keyValue[1]);
                }
            }
        }
        return new TestContext(facetMode);
    }

    public enum FacetMode {
        UNDEFINED,
        CLASSIC,
        SANDBOX,  // TODO: better names?
    }

}
