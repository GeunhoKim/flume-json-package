package com.geunho.apache.flume.intercepter;

import com.geunho.apache.flume.interceptor.JsonInterceptor.Constants;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Created by geunho on 3/21/16.
 */
public class JsonInterceptor {
  @Test
  public void shouldHaveTimestampInHeader() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String name = "com.geunho.apache.flume.interceptor.JsonInterceptor$Builder";
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(name);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("{\"timestamp\":\"2016-03-21T01:08:00.212+09:00\", \"message\":\"test message.\"}", StandardCharsets.UTF_8);

    Assert.assertNull(event.getHeaders().get(Constants.TIMESTAMP));

    Long now = System.currentTimeMillis();
    event = interceptor.intercept(event);
    String ts = event.getHeaders().get(Constants.TIMESTAMP);

    Assert.assertNotNull(ts);
    Assert.assertTrue(Long.parseLong(ts) <= now);
  }
}
