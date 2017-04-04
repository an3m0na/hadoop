package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDispatcher implements Dispatcher {
  private final Logger LOG = Logger.getLogger(SimpleDispatcher.class);
  private final Map<Class<? extends Enum>, EventHandler> eventDispatchers = new HashMap<>();
  private EventHandler handlerInstance;

  @Override
  public EventHandler getEventHandler() {
    if (handlerInstance == null) {
      handlerInstance = new GenericEventHandler();
    }
    return handlerInstance;
  }

  @Override
  public void register(Class<? extends Enum> eventType, EventHandler handler) {
/* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler = (EventHandler<Event>) eventDispatchers.get(eventType);
    if (registeredHandler == null) {
      eventDispatchers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)) {
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventDispatchers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }

  private class GenericEventHandler implements EventHandler<Event> {
    public void handle(Event event) {
      Class<? extends Enum> type = event.getType().getDeclaringClass();
      try {
        EventHandler handler = eventDispatchers.get(type);
        if (handler != null) {
          handler.handle(event);
        } else {
          throw new Exception("No handler for registered for " + type);
        }
      } catch (Throwable t) {
        LOG.error("Error encountered while handling event: " + event, t);
      }
    }
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   *
   * @param <T> the type of event these multiple handlers are interested in.
   */
  static class MultiListenerHandler<T extends Event> implements EventHandler<T> {
    List<EventHandler<T>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<T>>();
    }

    @Override
    public void handle(T event) {
      for (EventHandler<T> handler : listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<T> handler) {
      listofHandlers.add(handler);
    }

  }
}
