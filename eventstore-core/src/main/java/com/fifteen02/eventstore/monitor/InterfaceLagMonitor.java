package com.fifteen02.eventstore.monitor;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InterfaceLagMonitor {

    private static final Logger log = LoggerFactory.getLogger(InterfaceLagMonitor.class);

    private final InterfaceStateService interfaceStateService;
    private final IndexService indexService;
    private final InterfacesConfigProperties interfacesConfig;

    @Autowired
    public InterfaceLagMonitor(InterfaceStateService interfaceStateService,
                               IndexService indexService,
                               InterfacesConfigProperties interfacesConfig) {
        this.interfaceStateService = interfaceStateService;
        this.indexService = indexService;
        this.interfacesConfig = interfacesConfig;
    }

    /**
     * Monitors interface lag every 30 seconds.
     * Compares the latest interface identifiers with the persistor current index
     * and logs warnings when interfaces fall behind.
     */
    @Scheduled(fixedDelay = 30000) // 30 seconds
    public void monitorInterfaceLag() {
        try {
            // Get the current index
            long currentPersistorIndex = indexService.current().value();

            // Check lag on all acked interfaces except realtime.
            List<InterfacesConfigProperties.InterfaceDefinition> monitoredInterfaces = interfacesConfig.getInterfaces()
                    .stream()
                    .filter(def -> def.getType() != com.fifteen02.eventstore.config.InterfaceType.realtime)
                    .filter(def -> def.getAck() != null)
                    .toList();

            for (InterfacesConfigProperties.InterfaceDefinition interfaceDef : monitoredInterfaces) {
                String interfaceName = interfaceDef.getName();

                try {
                    // Get the last acknowledged index for this interface
                    long lastAckIndex = interfaceStateService.getLastAck(interfaceName);

                    // Calculate lag
                    long lag = currentPersistorIndex - lastAckIndex;

                    long batch = interfaceDef.getAck().getBatch();
                    long ratioThreshold = (long) Math.ceil(batch * interfaceDef.getAck().getLagAlertBatchRatio());
                    long threshold = Math.max(interfaceDef.getAck().getLagAlertMinMessages(), ratioThreshold);

                    // Alert when lag crosses the interface-specific threshold.
                    if (lag > threshold) {
                        // Severe lag - error level
                        log.error(
                                "[INTERFACE_BEHIND] Interface '{}' is behind: lag={} messages (current={}, lastAck={}, threshold={}, batch={}, ratio={})",
                                interfaceName,
                                lag,
                                currentPersistorIndex,
                                lastAckIndex,
                                threshold,
                                batch,
                                interfaceDef.getAck().getLagAlertBatchRatio()
                        );
                    }
                } catch (Exception e) {
                    log.error("Error checking lag for interface '{}': {}", interfaceName, e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            log.error("Error during interface lag monitoring: {}", e.getMessage(), e);
        }
    }
}
