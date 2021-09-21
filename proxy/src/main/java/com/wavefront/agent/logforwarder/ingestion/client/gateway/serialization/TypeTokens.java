package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:23 AM
 */
public interface TypeTokens {
  Type COLLECTION_OF_OBJECTS = (new TypeToken<Collection<Object>>() {
  }).getType();
  Type SET_OF_OBJECTS = (new TypeToken<Set<Object>>() {
  }).getType();
  Type LIST_OF_OBJECTS = (new TypeToken<List<Object>>() {
  }).getType();
  Type MAP_OF_STRINGS_BY_STRING = (new TypeToken<Map<String, String>>() {
  }).getType();
  Type MAP_OF_OBJECTS_BY_STRING = (new TypeToken<Map<String, Object>>() {
  }).getType();
}

