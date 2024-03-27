/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestCensusCatalog extends TestRESTCatalog {
  private RESTCatalog restCatalog;

  @BeforeEach
  public void createCatalog() throws Exception {
    String censusUri = "http://localhost:8080/catalog";

    // in the future we may want to add user info here
    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();

    this.restCatalog =
        new RESTCatalog(
            context,
            (config) -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
    restCatalog.initialize(
            "census",
            ImmutableMap.of(
                    CatalogProperties.URI, censusUri,
                    CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"
            ));
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }
  }


  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return false;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return false;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

}
