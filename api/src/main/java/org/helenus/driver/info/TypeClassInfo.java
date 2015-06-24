package org.helenus.driver.info;

/**
 * The <code>TypeClassInfo</code> interface extends on the {@link ClassInfo}
 * interface to provide addition information for type entity POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 11, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
public interface TypeClassInfo<T> extends ClassInfo<T> {
  /**
   * Gets the class info for the root entity defined for this type entity.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> root entity POJO class info defined for
   *         this type entity
   */
  public RootClassInfo<? super T> getRoot();

  /**
   * Gets the type of this POJO class.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> type of this POJO class
   */
  public String getType();
}
