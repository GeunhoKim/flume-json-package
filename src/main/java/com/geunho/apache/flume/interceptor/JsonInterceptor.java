package com.geunho.apache.flume.interceptor;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.*;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.geunho.apache.flume.interceptor.JsonInterceptor.Constants.*;
/**
 * Created by geunho on 3/20/16.
 */
public class JsonInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
    .getLogger(JsonInterceptor.class);

  private final Gson gson;
  private final String timestamp;

  private JsonInterceptor(String timestampFieldName) {
    this.timestamp = timestampFieldName;
    this.gson = new Gson();
  }

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();

    String payload = new String(event.getBody(), StandardCharsets.UTF_8);
    Map<String, Object> json = parseJson(payload);

    if(json.containsKey(timestamp)) {
      String ts = (String)json.get(timestamp);
      DateTime dt = ISODateTimeFormat.dateTime().parseDateTime(ts);
      long tm = dt.getMillis();
      headers.put(timestamp, Long.toString(tm));

    } else {
      long now = System.currentTimeMillis();
      headers.put(timestamp, Long.toString(now));
    }

    return event;
  }

  private Map<String, Object> parseJson(String payload) {
    return gson.fromJson(payload, Map.class);
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }

    return events;
  }

  @Override
  public void close() {
  }

  public static class Builder implements Interceptor.Builder {
    private String timestampFieldName = TIMESTAMP;

    @Override
    public void configure(Context context) {
      timestampFieldName = context.getString(TIMESTAMP_FIELD, TIMESTAMP);
    }

    @Override
    public Interceptor build() {
      return new JsonInterceptor(timestampFieldName);
    }
  }

  public static class Constants {
    public static String TIMESTAMP_FIELD = "timestampFieldName";
    public static String TIMESTAMP = "timestamp";
  }
}
