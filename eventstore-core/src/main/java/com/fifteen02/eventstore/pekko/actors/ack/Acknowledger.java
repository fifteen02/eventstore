package com.fifteen02.eventstore.pekko.actors.ack;

import com.fifteen02.eventstore.pekko.actors.interfaces.Interface;

public interface Acknowledger {
    void ack(Interface.ExternalAck message);
}
