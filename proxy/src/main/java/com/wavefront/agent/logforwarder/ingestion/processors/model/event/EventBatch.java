package com.wavefront.agent.logforwarder.ingestion.processors.model.event;


import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * List of Events
 */
public class EventBatch extends ArrayList<Event> {

  private boolean isDirty = false;

  public EventBatch() {
    super();
  }

  public boolean add(Event event) {
    this.isDirty = true;
    return super.add(event);
  }

  @Override
  public boolean addAll(Collection<? extends Event> events) {
    this.isDirty = true;
    return super.addAll(events);
  }

  @Override
  public void add(int index, Event element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(int index, Collection<? extends Event> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    this.isDirty = true;
    return super.remove(o);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    this.isDirty = true;
    return super.removeAll(c);
  }

  @Override
  public boolean removeIf(Predicate<? super Event> filter) {
    this.isDirty = super.removeIf(filter);
    return this.isDirty;
  }

  @Override
  public Event remove(int index) {
    this.isDirty = true;
    return super.remove(index);
  }

  @Override
  protected void removeRange(int fromIndex, int toIndex) {
    this.isDirty = true;
    super.removeRange(fromIndex, toIndex);
  }

  @Override
  public Event set(int index, Event element) {
    this.isDirty = true;
    return super.set(index, element);
  }

  /**
   * @return true if {@link Event} is added/deleted from the batch
   * or any fields are added/updated/deleted from {@link Event}
   */
  public boolean isDirty() {
    boolean isDirty = this.isDirty;

    for (Event e : this) {
      if (isDirty) {
        break;
      }

      isDirty = isDirty | e.isDirty();
    }

    return isDirty;
  }

  public void setDirty(boolean dirty) {
    isDirty = dirty;
    this.forEach(event -> event.setDirty(dirty));
  }
}