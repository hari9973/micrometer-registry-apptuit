package ai.apptuit;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Timer;

public class ApptuitMeterRegistryTest {
	private MockClock clock = new MockClock();
    private ApptuitConfig config = new ApptuitConfig() {
        @Override
        public String get(String key) {
            return null;
        }

        @Override
        public boolean enabled() {
            return false;
        }
    };
    private ApptuitMeterRegistry registry = new ApptuitMeterRegistry(config, clock);
    ApptuitMeterRegistry.DataPointCollector collector = registry.new DataPointCollector(System.currentTimeMillis() / 1000);
    Random r = new Random();
    private int rangeMin = 1;
    private int rangeMax = 1000;
     
    @Test
    public void testCounter() {
    	Counter counter = Counter.builder("my.Counter").register(registry);
        counter.increment();
        counter.increment();
        clock.add(config.step());
        collector.collectCounter(counter.getId().getName(),counter);
        assertEquals(1,collector.dataPoints.size());
        assertEquals(2.0,collector.dataPoints.get(0).getValue());
        assertEquals("my_Counter_total",collector.dataPoints.get(0).getMetric());
        collector.dataPoints.clear();
    }
    
    @Test
    public void testGuage() {
    	double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
    	Gauge gauge = Gauge.builder("my.Gauge", randomValue, Number::doubleValue).register(registry);
    	collector.collectGauge(gauge.getId().getName(),gauge);
    	assertEquals(1,collector.dataPoints.size());
    	assertEquals(randomValue,collector.dataPoints.get(0).getValue());
    	assertEquals("my_Gauge",collector.dataPoints.get(0).getMetric());
    	collector.dataPoints.clear();
    }
    
    @Test
    public void testFunctionCounter() {
    	FunctionCounter counter = FunctionCounter.builder("my.Function.Counter",0.0, Number::doubleValue).register(registry);
    	collector.collectFunctionCounter(counter.getId().getName(), counter);
    	assertEquals(1,collector.dataPoints.size());
    	assertEquals(0.0,collector.dataPoints.get(0).getValue());
    	assertEquals("my_Function_Counter_total",collector.dataPoints.get(0).getMetric());
    	collector.dataPoints.clear();
    }
    
    @Test
    public void testHistogram() {
    	DistributionSummary histogram = DistributionSummary.builder("my.Histogram").register(registry);
    	collector.collectHistogram(histogram.getId().getName(), histogram);
    	assertEquals(3,collector.dataPoints.size());
    	assertEquals(0.0,collector.dataPoints.get(0).getValue());
    	assertEquals("my_Histogram_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration_max",collector.dataPoints.get(0).getMetric());
    	assertEquals("my_Histogram_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration_mean",collector.dataPoints.get(1).getMetric());
    	assertEquals("my_Histogram_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration_count",collector.dataPoints.get(2).getMetric());
    	collector.dataPoints.clear();
    }
    
    @Test
    public void testTimer() {
    	Timer timer = Timer.builder("my.Timer").register(registry);
    	collector.collectTimer(timer.getId().getName(), timer);
    	assertEquals(3,collector.dataPoints.size());
    	assertEquals(0.0,collector.dataPoints.get(0).getValue());
    	assertEquals("my_Timer_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration_max",collector.dataPoints.get(0).getMetric());
    	assertEquals("my_Timer_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration_mean",collector.dataPoints.get(1).getMetric());
    	assertEquals("my_Timer_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration_count",collector.dataPoints.get(2).getMetric());
    	collector.dataPoints.clear();
    }
    
    @Test
    public void testLongTaskTimer() {
    	LongTaskTimer timer = LongTaskTimer.builder("my.LongTaskTimer").register(registry);
    	collector.collectLongTaskTimer(timer.getId().getName(), timer);
    	assertEquals(2,collector.dataPoints.size());
    	assertEquals("my_LongTaskTimer_activeTasks",collector.dataPoints.get(0).getMetric());
    	assertEquals("my_LongTaskTimer_"+registry.getBaseTimeUnit().toString().toLowerCase()+"_duration",collector.dataPoints.get(1).getMetric());
    	collector.dataPoints.clear();
    }
    
    @Test
    public void testMeter() {
    	Meter meter = Meter.builder("my.Meter",Meter.Type.COUNTER,Collections.singletonList(new Measurement(() -> 1.0, Statistic.COUNT))).register(registry);
    	collector.collectMeter(meter.getId().getName(),meter);
    	assertEquals(1,collector.dataPoints.size());
    	assertEquals("my_Meter_count",collector.dataPoints.get(0).getMetric());
    	assertEquals(1.0,collector.dataPoints.get(0).getValue());
    	collector.dataPoints.clear();
    }
    
    
}
