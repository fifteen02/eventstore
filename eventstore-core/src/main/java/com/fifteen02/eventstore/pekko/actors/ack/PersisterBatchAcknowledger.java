package com.fifteen02.eventstore.pekko.actors.ack;

import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.service.PersistorService;

public class PersisterBatchAcknowledger extends BatchAcknowledger {

    private final PersistorService persistorService;

    public PersisterBatchAcknowledger(String interfaceName, InterfaceStateService interfaceStateService, PersistorService persistorService, long batch) {
        super(interfaceName, interfaceStateService, batch);
        this.persistorService = persistorService;
        persistorService.ensurePersister(interfaceName, false, 0);
    }

    @Override
    void updateAcknowledged(long lastIndexToPersist) {
        super.updateAcknowledged(lastIndexToPersist);
        persistorService.updatePersister(interfaceName, lastIndexToPersist);
    }
}
