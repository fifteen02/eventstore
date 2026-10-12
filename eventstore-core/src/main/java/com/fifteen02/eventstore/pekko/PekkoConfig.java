package com.fifteen02.eventstore.pekko;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PekkoConfig {

  @Bean
  public ActorRegistry actorRegistry() {
    return new ActorRegistry();
  }
}
