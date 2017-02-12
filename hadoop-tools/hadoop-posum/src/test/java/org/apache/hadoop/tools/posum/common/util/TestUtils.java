package org.apache.hadoop.tools.posum.common.util;

import org.junit.Test;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestUtils {

  private static final long defaultNoGetterProperty = 1234567L;

  public static class ComplicatedBean {
    private String aString = "default";
    private boolean done = false;
    private Long thisHasNoGetter = defaultNoGetterProperty;

    public String getaString() {
      return aString;
    }

    public void setaString(String aString) {
      this.aString = aString;
    }

    public boolean isDone() {
      return done;
    }

    public void setDone(boolean done) {
      this.done = done;
    }

    public void setThisHasNoGetter(Long thisHasNoGetter) {
      this.thisHasNoGetter = thisHasNoGetter;
    }

    public int getExtraProperty() {
      return 5;
    }
  }

  @Test
  public void testBeanPropertiesMatch() throws IllegalAccessException, IntrospectionException, InvocationTargetException {
    ComplicatedBean bean = new ComplicatedBean();
    Map<String, Object> properties = new HashMap<>(5);
    properties.put("aString", bean.getaString());
    properties.put("done", bean.isDone());
    properties.put("extraProperty", bean.getExtraProperty());
    assertTrue(Utils.checkBeanPropertiesMatch(bean, properties));
  }

  @Test
  public void testBeanPropertiesMatchAfterModification() throws IllegalAccessException, IntrospectionException, InvocationTargetException {
    ComplicatedBean bean = new ComplicatedBean();
    String newString = "modifiedString";
    bean.setaString(newString);
    bean.setDone(true);
    Map<String, Object> properties = new HashMap<>(5);
    properties.put("aString", newString);
    properties.put("done", true);
    properties.put("extraProperty", bean.getExtraProperty());
    assertTrue(Utils.checkBeanPropertiesMatch(bean, properties));
  }

  @Test(expected = PosumException.class)
  public void testBeanPropertiesNeedGetter() throws IllegalAccessException, IntrospectionException, InvocationTargetException {
    ComplicatedBean bean = new ComplicatedBean();
    Map<String, Object> properties = new HashMap<>(5);
    properties.put("thisHasNoGetter", defaultNoGetterProperty);
    Utils.checkBeanPropertiesMatch(bean, properties);
  }


}
