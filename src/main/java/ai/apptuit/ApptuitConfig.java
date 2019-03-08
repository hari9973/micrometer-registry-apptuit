package ai.apptuit;

import io.micrometer.core.instrument.step.StepRegistryConfig;


public interface ApptuitConfig extends StepRegistryConfig {

    ApptuitConfig DEFAULT = k -> null;

    String get(String key);

    default String prefix() {
        return "apptuit";
    }
    
    default String token() {
        String v = get(prefix() + ".token");
        return v == null ? "" : v;
    }
}
