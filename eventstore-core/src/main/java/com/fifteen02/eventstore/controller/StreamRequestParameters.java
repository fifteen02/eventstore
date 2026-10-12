package com.fifteen02.eventstore.controller;

import java.util.Set;

public record StreamRequestParameters(Set<String> messageType,
                                      Set<String> userIds,
                                      Long startIndex,
                                      Long endIndex,
                                      Integer shardCount,
                                      Integer shardIndex) {
}
