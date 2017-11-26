package org.apache.hadoop.tools.posum.common.util;

import org.apache.commons.lang.WordUtils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GeneralUtils {

  public static void writeField(Object object, Class startClass, String name, Object value) {
    try {
      Field field = GeneralUtils.findField(startClass, name);
      field.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      field.set(object, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new PosumException("Reflection error: ", e);
    }
  }

  private static Field findField(Class startClass, String name)
    throws NoSuchFieldException {
    Class crtClass = startClass;
    while (crtClass != null) {
      Field[] fields = crtClass.getDeclaredFields();
      for (Field field : fields) {
        if (field.getName().equals(name)) {
          return field;
        }
      }
      crtClass = crtClass.getSuperclass();
    }
    throw new NoSuchFieldException(startClass.getName() + "." + name);
  }

  private static Method findMethod(Class startClass, String name, Class<?>... paramTypes)
    throws NoSuchMethodException {
    Class crtClass = startClass;
    while (crtClass != null) {
      Method[] methods = crtClass.getDeclaredMethods();
      for (Method method : methods) {
        if (method.getName().equals(name)) {
          if (Arrays.equals(method.getParameterTypes(), paramTypes)) {
            return method;
          }
        }
      }
      crtClass = crtClass.getSuperclass();
    }
    throw new NoSuchMethodException(startClass.getName() + "." + name +
      (paramTypes != null ? Arrays.asList(paramTypes).toString().replace('[', '(').replace(']', ')') : ""));
  }

  public static <T> T readField(Object object, Class startClass, String name) {
    try {
      Field field = GeneralUtils.findField(startClass, name);
      field.setAccessible(true);
      return (T) field.get(object);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new PosumException("Reflection error: ", e);
    }
  }


  public static <T> T invokeMethod(Object object, Class startClass, String name, Class<?>[] paramTypes, Object... args) {
    try {
      Method method = GeneralUtils.findMethod(startClass, name, paramTypes);
      method.setAccessible(true);
      return (T) method.invoke(object, args);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
      throw new PosumException("Reflection error: ", e);
    }
  }

  public static boolean safeEquals(Object o1, Object o2) {
    if (o1 == null)
      return o2 == null;
    return o1.equals(o2);
  }

  public static int safeHashCode(Object o) {
    return o == null ? 0 : o.hashCode();
  }

  public static Map<String, Method> getBeanPropertyReaders(Class beanClass,
                                                           Set<String> propertyNames)
    throws IntrospectionException {
    Map<String, Method> ret = new HashMap<>(propertyNames.size());
    PropertyDescriptor[] descriptors =
      Introspector.getBeanInfo(beanClass, Object.class).getPropertyDescriptors();
    for (String name : propertyNames) {
      Method reader = findPropertyReader(descriptors, name);
      if (reader == null) {
        // explore name variations
        String alternatePropertyName = name.startsWith("_") ? name.substring(1) : "_" + name;
        reader = findPropertyReader(descriptors, alternatePropertyName);
        if (reader == null) {
          if (name.contains("_"))
            alternatePropertyName = WordUtils.capitalizeFully(name).replaceAll("_", "");
          else
            alternatePropertyName = name.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
          reader = findPropertyReader(descriptors, alternatePropertyName);
        }
      }
      if (reader == null)
        throw new PosumException("Could not find property reader for " + name + " in " + beanClass);
      ret.put(name, reader);
    }
    return ret;
  }

  private static Method findPropertyReader(PropertyDescriptor[] propertyDescriptors, String propertyName) {
    for (PropertyDescriptor pd : propertyDescriptors) {
      if (propertyName.equals(pd.getName())) {
        return pd.getReadMethod();
      }
    }
    return null;
  }

  public static long orZero(Long unsafeLong) {
    return unsafeLong == null ? 0 : unsafeLong;
  }

  public static float orZero(Float unsafeFloat) {
    return unsafeFloat == null ? 0 : unsafeFloat;
  }

  public static double orZero(Double unsafeDouble) {
    return unsafeDouble == null ? 0 : unsafeDouble;
  }

  public static int orZero(Integer unsafeInt) {
    return unsafeInt == null ? 0 : unsafeInt;
  }
}
