package ai.apptuit;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.apptuit.metrics.client.ApptuitPutClient;
import ai.apptuit.metrics.client.DataPoint;
import ai.apptuit.metrics.dropwizard.TagEncodedMetricName;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.TimeUtils;
import io.micrometer.core.lang.NonNull;

public class ApptuitMeterRegistry extends StepMeterRegistry {
    private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory("apptuit-metrics-publisher");

    private final Logger logger = LoggerFactory.getLogger(ApptuitMeterRegistry.class);

    private final ApptuitConfig config;
    
	private final Map<String, String> GLOBAL_TAGS_MAP = new HashMap<String, String>();
	
	private ApptuitPutClient putClient;
	
	public DataPointCollector testing = new DataPointCollector(System.currentTimeMillis() / 1000);

    @SuppressWarnings("deprecation")
    public ApptuitMeterRegistry(ApptuitConfig config, Clock clock) {
        this(config, clock, DEFAULT_THREAD_FACTORY);
    }

    private ApptuitMeterRegistry(ApptuitConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        this.config = config;
        putClient = getClient();
        GLOBAL_TAGS_MAP.put("process", "spring-boot-application");
        start(threadFactory);
    }

    public static Builder builder(ApptuitConfig config) {
        return new Builder(config);
    }
    
    private final ApptuitPutClient getClient(){
    	return  new ApptuitPutClient(config.token(), GLOBAL_TAGS_MAP);
    }

    @Override
    public void start(ThreadFactory threadFactory) {
        if (config.enabled()) {
            logger.info("publishing metrics to apptuit every " + TimeUtils.format(config.step()));
        }
        super.start(threadFactory);
    }

    @Override
    protected void publish() {
        for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
            try {
                logger.error(String.valueOf(batch.size()));
                DataPointCollector collector = new DataPointCollector(System.currentTimeMillis() / 1000);
                for(int i=0;i<batch.size();i++) {
                	if(batch.get(i) instanceof Gauge) {
                		collector.collectGauge(batch.get(i).getId().getName(),(Gauge)batch.get(i));
                	}else if(batch.get(i) instanceof Counter) {
                		collector.collectCounter(batch.get(i).getId().getName(),(Counter)batch.get(i));
                	}else if(batch.get(i) instanceof DistributionSummary) {
                		collector.collectHistogram(batch.get(i).getId().getName(),(DistributionSummary)batch.get(i));
                	}else if(batch.get(i) instanceof Timer){
                		collector.collectTimer(batch.get(i).getId().getName(),(Timer)batch.get(i));
                	}else if(batch.get(i) instanceof LongTaskTimer) {
                		collector.collectLongTaskTimer(batch.get(i).getId().getName(),(LongTaskTimer)batch.get(i));
                	}else if(batch.get(i) instanceof FunctionTimer) {
                		collector.collectFunctionTimer(batch.get(i).getId().getName(),(FunctionTimer)batch.get(i));
                	}else if(batch.get(i) instanceof FunctionCounter){
                		collector.collectFunctionCounter(batch.get(i).getId().getName(),(FunctionCounter)batch.get(i));
                	}else if(batch.get(i) instanceof Meter) {
                		collector.collectMeter(batch.get(i).getId().getName(),(Meter)batch.get(i));
                	}
                }
                try {
                	putClient.put(collector.dataPoints);
                } catch(Throwable e) {
                	logger.error("failed to send metrics to apptuit", e);
                }
            } catch (Throwable e) {
                logger.error("failed to send metrics to apptuit", e); 
            }
        }
    }
    
    protected class DataPointCollector {

        private final long epoch;
        protected final List<DataPoint> dataPoints;

        DataPointCollector(long epoch) {
          this.epoch = epoch;
          this.dataPoints = new LinkedList<>();
        }

        private String addBaseUnitToName(String name,Meter m) {
        	if(m.getId().getBaseUnit() != null) {
        		return name+"."+m.getId().getBaseUnit();
        	}
        	return name;
        }
        
        private String getsSanitizedName(String name) {
        	return name.replace(".", "_");
        }
    
        protected void collectGauge(String name, Gauge gauge) {
		    Object value = gauge.value();
		    if (value instanceof BigDecimal) {
		      addDataPoint(addBaseUnitToName(name,gauge), ((BigDecimal) value).doubleValue(),gauge.getId().getTags());
		    } else if (value instanceof BigInteger) {
		      addDataPoint(addBaseUnitToName(name,gauge), ((BigInteger) value).doubleValue(),gauge.getId().getTags());
		    } else if (value != null && value.getClass().isAssignableFrom(Double.class)) {
		      if (!Double.isNaN((Double) value) && Double.isFinite((Double) value)) {
		        addDataPoint(addBaseUnitToName(name,gauge), (Double) value,gauge.getId().getTags());
		      }
		    } else if (value instanceof Number) {
		      addDataPoint(addBaseUnitToName(name,gauge), ((Number) value).doubleValue(),gauge.getId().getTags());
		    }
		}
    
	    protected void collectCounter(String name, Counter counter) {
	        addDataPoint(name+".total", counter.count(),counter.getId().getTags());
	    }
	    
	    protected void collectFunctionCounter(String name,FunctionCounter counter) {
	    	addDataPoint(name+".total", counter.count(),counter.getId().getTags());
	    }
	    
	    protected void collectHistogram(String name, DistributionSummary histogram) {
	        TagEncodedMetricName rootMetric = TagEncodedMetricName.decode(name);
	        reportSnapshot(rootMetric, histogram.takeSnapshot(),histogram.getId().getTags());
	    }
	    
	    protected void collectTimer(String name, Timer timer) {
	        TagEncodedMetricName rootMetric = TagEncodedMetricName.decode(name);
	        reportSnapshot(rootMetric, timer.takeSnapshot(),timer.getId().getTags());
	     }
	    
	    protected void collectLongTaskTimer(String name, LongTaskTimer longTimer) {
	        addDataPoint(name+".activeTasks",(double)longTimer.activeTasks(),longTimer.getId().getTags());
	        addDataPoint(name+"."+getBaseTimeUnit().toString().toLowerCase()+".duration",longTimer.duration(getBaseTimeUnit()),longTimer.getId().getTags());
	     }
	    protected void collectFunctionTimer(String name,FunctionTimer funcTimer) {
	    	addDataPoint(name+"."+getBaseTimeUnit().toString().toLowerCase()+".count",funcTimer.count(),funcTimer.getId().getTags());
	    	addDataPoint(name+"."+getBaseTimeUnit().toString().toLowerCase()+".sum",funcTimer.totalTime(getBaseTimeUnit()),funcTimer.getId().getTags());
	    	addDataPoint(name+"."+getBaseTimeUnit().toString().toLowerCase()+".mean",funcTimer.mean(getBaseTimeUnit()),funcTimer.getId().getTags());
	    }
	    
	    protected void collectMeter(String name,Meter meter) {
	    	for (Measurement measurement : meter.measure()) {
	    		this.addDataPoint(name+"."+measurement.getStatistic().toString().toLowerCase(), measurement.getValue(), meter.getId().getTags());
	    	}
	    }
	   	
	    private void reportSnapshot(TagEncodedMetricName metric, HistogramSnapshot snapshot,List<Tag> tags) {
			addDataPoint(metric.submetric(getBaseTimeUnit().toString().toLowerCase()+".duration.max"), snapshot.max(getBaseTimeUnit()),tags);
			addDataPoint(metric.submetric(getBaseTimeUnit().toString().toLowerCase()+".duration.mean"), snapshot.mean(getBaseTimeUnit()),tags);
			addDataPoint(metric.submetric(getBaseTimeUnit().toString().toLowerCase()+".duration.count"), snapshot.total(),tags);
			ValueAtPercentile[] percentValues = snapshot.percentileValues();
			for(ValueAtPercentile obj: percentValues){
				addDataPoint(metric.submetric(getBaseTimeUnit().toString().toLowerCase()+".duration").withTags("quantile", Double.toString(obj.percentile())),
						convertDuration(obj.value()),tags);
			}
		}
	    
	    private double convertDuration(double duration) {
	        return this.convertDuration(duration);
	    }

	    private void addDataPoint(String name, double value,List<Tag> tags) {
	      addDataPoint(TagEncodedMetricName.decode(name), value,tags);
        }

	   private void addDataPoint(TagEncodedMetricName name, Number value,List<Tag> tags) {
	    	Map<String, String> tag = new HashMap<String, String>();
	    	for(Tag obj:tags) {
	    		tag.put(obj.getKey(), obj.getValue());
	    	}
	    	DataPoint dataPoint;
	    	if(tag.isEmpty()) {
	    		dataPoint = new DataPoint(getsSanitizedName(name.getMetricName()), epoch, value, name.getTags());
	    	}
	    	else {
	    		dataPoint = new DataPoint(getsSanitizedName(name.getMetricName()), epoch, value, tag);
	    	}
	        dataPoints.add(dataPoint);
	        logger.error(dataPoint.getMetric()+dataPoint.getTags()+dataPoint.getValue());
	    }
    }
    
    @Override
    @NonNull
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    public static class Builder {
        private final ApptuitConfig config;

        private Clock clock = Clock.SYSTEM;
        private ThreadFactory threadFactory = DEFAULT_THREAD_FACTORY;

        @SuppressWarnings("deprecation")
        Builder(ApptuitConfig config) {
            this.config = config;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public ApptuitMeterRegistry build() {
            return new ApptuitMeterRegistry(config, clock, threadFactory);
        }
    }
}