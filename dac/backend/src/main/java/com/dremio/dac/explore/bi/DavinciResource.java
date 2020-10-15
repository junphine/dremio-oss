/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.dac.explore.bi;

import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_PBIDS;
import static com.dremio.dac.server.WebServer.MediaType.TEXT_PLAIN_QLIK_APP;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.resource.BaseBIToolResource;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;

/**
 * Resource to create qlik load script for a given dataset
 *
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/davinci/{datasetId}")
@Options
public class DavinciResource extends BaseBIToolResource {
  // Special option for enabling the Power BI PBIDS endpoint.
  public static final TypeValidators.BooleanValidator CLIENT_TOOLS_DAVINCI
    = new TypeValidators.BooleanValidator("client.tools.davinci", true);
  public static final TypeValidators.BooleanValidator ALLOW_DAVINCI
    = new TypeValidators.BooleanValidator("support.dac.davinci", true);

  private final NamespaceService namespaceService;
  private final ProjectOptionManager optionManager;
  private final String datasetId;

  @Inject
  public DavinciResource(
      NamespaceService namespaceService,
      ProjectOptionManager optionManager,
      @PathParam("datasetId") String datasetId) {
    super(namespaceService, optionManager, datasetId);
    this.namespaceService = namespaceService;
    this.optionManager = optionManager;
    this.datasetId = datasetId;
  }

  /**
   * returns a Qlik load script for the dataset
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @GET
  @Produces(APPLICATION_PBIDS)
  public Response get(@HeaderParam(HttpHeaders.HOST) String host) throws DatasetNotFoundException, NamespaceException {
    if (!optionManager.getOption(ALLOW_DAVINCI)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return getWithHostHelper(host);
  }

  @Override
  protected TypeValidators.BooleanValidator getClientToolOption() {
    return CLIENT_TOOLS_DAVINCI;
  }
}
