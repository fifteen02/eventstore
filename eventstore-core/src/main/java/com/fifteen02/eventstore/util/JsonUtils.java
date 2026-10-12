package com.fifteen02.eventstore.util;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonUtils {
  @Bean
  public ObjectMapper objectMapper() {
    return JsonMapper.builderWithJackson2Defaults().build();
  }
}
