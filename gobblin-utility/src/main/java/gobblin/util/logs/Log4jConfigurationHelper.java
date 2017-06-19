/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.util.logs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;


/**
 * A helper class for programmatically configuring log4j.
 *
 * @author Yinan Li
 */
public class Log4jConfigurationHelper {

  /**
   * Update the log4j configuration by loading the specified log4jFileName as a resource from the classpath of the
   * targetClass.
   *
   * @param targetClass the target class used to get the original log4j configuration file as a resource
   * @param log4jFileName the custom log4j configuration properties file name
   * @throws IOException if there's something wrong with updating the log4j configuration
   */
  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jFileName)
          throws IOException {
    Properties originalProperties = loadLog4jFileFromResource(targetClass, log4jFileName);
    LogManager.resetConfiguration();
    PropertyConfigurator.configure(originalProperties);
  }

  /**
   * Update the log4j configuration by loading the specified log4jFileName as a resource from the classpath of the
   * targetClass and then loading log4jPath from the local filesystem.
   *
   * @param targetClass the target class used to get the original log4j configuration file as a resource
   * @param log4jPath the custom log4j configuration properties file path
   * @param log4jFileName the custom log4j configuration properties file name
   * @throws IOException if there's something wrong with updating the log4j configuration
   */
  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jPath, String log4jFileName)
          throws IOException {
    Properties customProperties = loadLog4jFile(log4jPath);
    Properties originalProperties = loadLog4jFileFromResource(targetClass, log4jFileName);

    for (Entry<Object, Object> entry : customProperties.entrySet()) {
      originalProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
    }

    LogManager.resetConfiguration();
    PropertyConfigurator.configure(originalProperties);
  }

  private static Properties loadLog4jFile(String log4jPath) throws IOException {
    try (InputStream fileInputStream = new FileInputStream(log4jPath)) {
      Properties customProperties = new Properties();
      customProperties.load(fileInputStream);
      return customProperties;
    }
  }

  private static Properties loadLog4jFileFromResource(Class<?> targetClass, String log4jFileName) throws IOException {
    try (InputStream inputStream = targetClass.getResourceAsStream("/" + log4jFileName)) {
      Properties originalProperties = new Properties();
      originalProperties.load(inputStream);
      return originalProperties;
    }
  }
}
