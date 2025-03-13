/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;


// A DataSource implementation that uses DriverManager
public class DriverManagerDataSource implements DataSource {

  private String jdbcUrl;
  private Properties properties;

  public DriverManagerDataSource() {
    this(null, null);
  }

  /**
   * Create a new DriverManagerDataSource with the given JDBC URL and properties.
   *
   * @param url the JDBC URL to use for accessing the DriverManager
   * @param props the JDBC connection properties
   * @see DriverManager#getConnection(String)
   */
  public DriverManagerDataSource(String url, Properties props) {
    this.jdbcUrl = url;
    this.properties = props;
  }

  @Override
  public Connection getConnection() throws SQLException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jdbcUrl), "JDBC URL must not be null or empty.");
    return DriverManager.getConnection(jdbcUrl, properties);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return DriverManager.getConnection(jdbcUrl, username, password);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {}

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {}

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
