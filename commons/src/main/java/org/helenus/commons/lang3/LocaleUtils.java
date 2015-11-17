/*
 * Copyright (C) 2015-2015 The Helenus Driver Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helenus.commons.lang3;

import java.util.IllformedLocaleException;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;

import sun.util.locale.InternalLocaleBuilder;
import sun.util.locale.LocaleExtensions;
import sun.util.locale.LocaleSyntaxException;

/**
 * The <code>LocaleUtils</code> class extends Apache's
 * {@link org.apache.commons.lang3.LocaleUtils} to either provide new
 * functionality or replace existing one.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Nov 16, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@SuppressWarnings("restriction")
public class LocaleUtils extends org.apache.commons.lang3.LocaleUtils {
  /**
   * Instantiates a new <code>LocaleUtils</code> object.
   *
   * @author paouelle
   */
  public LocaleUtils() {}

  /**
   * Creates a locale for the specified locale string.
   *
   * @author paouelle
   *
   * @param  str a non-<code>null</code> locale string to convert
   * @param  ostr the non-<code>null</code> original locale string
   * @return a corresponding non-<code>null</code> locale
   * @throws IllegalArgumentException if the string is an invalid format
   */
  private static Locale getLocale(String str, String ostr) {
    if (str.isEmpty()) {
      return Locale.ROOT;
    }
    final int len = str.length();

    if (len < 2) {
      throw new IllegalArgumentException("invalid locale format: " + ostr);
    }
    final char ch0 = str.charAt(0);

    if (ch0 == '_') {
      if (len < 3) {
        throw new IllegalArgumentException("invalid locale format: " + ostr);
      }
      final char ch1 = str.charAt(1);
      final char ch2 = str.charAt(2);

      if (!Character.isUpperCase(ch1) || !Character.isUpperCase(ch2)) {
        throw new IllegalArgumentException("invalid locale format: " + ostr);
      }
      if (len == 3) {
        return new Locale("", str.substring(1, 3));
      }
      if (len < 5) {
        throw new IllegalArgumentException("invalid locale format: " + ostr);
      }
      if (str.charAt(3) != '_') {
        throw new IllegalArgumentException("invalid locale format: " + ostr);
      }
      return new Locale("", str.substring(1, 3), str.substring(4));
    }
    final int i = str.indexOf('_');

    if (i == -1) { // only a language
      if (StringUtils.isAllLowerCase(str) && ((len == 2) || (len == 3))) {
        return new Locale(str);
      }
    } else {
      final String lang = str.substring(0, i);
      final String lstr = str.substring(i + 1);
      final int j = lstr.indexOf('_');

      if (j == -1) { // no variants
        if (StringUtils.isAllLowerCase(lang)
            && ((lang.length() == 2) || (lang.length() == 3))
            && ((lstr.length() == 0)
                || ((lstr.length() == 2) && StringUtils.isAllUpperCase(lstr)))) {
          return new Locale(lang, lstr);
        }
      } else {
        final String reg = lstr.substring(0, j);
        final String variant = lstr.substring(j + 1);

        if (StringUtils.isAllLowerCase(lang)
            && ((lang.length() == 2) || (lang.length() == 3))
            && ((reg.length() == 0)
                || ((reg.length() == 2) && StringUtils.isAllUpperCase(reg)))) {
          return new Locale(lang, reg, variant);
        }
      }
    }
    throw new IllegalArgumentException("invalid locale format: " + ostr);
  }

  /**
   * Converts a string to a Locale.
   * <p>
   * This method takes the string format of a locale and creates the locale
   * object from it. It also properly supports scripts and extensions. It also
   * fixes some problems with Apache's version which didn't properly support
   * locale without a country but with an extension. It also ensures that all
   * locales that are returned by {@link Locale#getAvailableLocales} can be
   * properly decoded and returned in such a way that the {@link Locale#equals}
   * method will result to <code>true</code>.
   * <p>
   * <pre>
   *   LocaleUtils.toLocale("")           = Locale.ROOT
   *   LocaleUtils.toLocale("en")         = new Locale("en", "")
   *   LocaleUtils.toLocale("en_GB")      = new Locale("en", "GB")
   *   LocaleUtils.toLocale("en_GB_xxx")  = new Locale("en", "GB", "xxx")   (#)
   * </pre>
   * <p>
   * This method validates the input strictly.
   * The language code must be lowercase.
   * The country code must be uppercase.
   * The separator must be an underscore.
   * The length must be correct.
   *
   * @author paouelle
   *
   * @param  str the locale string to convert
   * @return a corresponding locale or <code>null</code> if <code>str</code> is
   *         <code>null</code>
   * @throws IllegalArgumentException if the string is an invalid format
   */
  public static Locale toLocale(String str) {
    if (str == null) {
      return null;
    }
    try {
      final int i = str.indexOf("_#");

      if (i != -1) { // split it to handle script/extensions separately
        final Locale locale = LocaleUtils.getLocale(str.substring(0, i), str);
        final String script_extn = str.substring(i + 2);

        if (locale.toString().equals(str)) {
          // it is already what it should be so return it
          // this will work around the fact that the JDK returns ja_JP_JP_#u-ca-japanese
          // as a valid locale but if you try to re-create it it will automatically
          // empty the variant and end up creating ja_JP_#u-ca-japanese instead
          return locale;
        }
        final Locale.Builder builder = new Locale.Builder();

        builder.setLocale(locale);
        final int j = script_extn.indexOf('_');
        final String extns;

        if (j == -1) { // only script or extensions; script must be 4
          final int k = script_extn.indexOf('-');

          if (k == -1) { // we have only a script
            builder.setScript(script_extn);
            extns = null;
          } else { // we have extensions
            extns = script_extn;
          }
        } else { // we have both
          builder.setScript(script_extn.substring(0, j));
          extns = script_extn.substring(j + 1);
        }
        if (extns != null) {
          final InternalLocaleBuilder ibldr = new InternalLocaleBuilder();

          ibldr.setExtensions(extns);
          final LocaleExtensions lextns = ibldr.getLocaleExtensions();

          lextns.getKeys().forEach(k -> builder.setExtension(k, lextns.getExtensionValue(k)));
        }
        return builder.build();
      } // else - no extensions or scripts to deal with
      return LocaleUtils.getLocale(str, str);
    } catch (IllformedLocaleException|LocaleSyntaxException e) {
      throw new IllegalArgumentException("invalid locale format: " + str, e);
    }
  }
}
