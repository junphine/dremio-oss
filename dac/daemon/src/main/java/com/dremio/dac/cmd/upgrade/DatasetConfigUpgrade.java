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

package com.dremio.dac.cmd.upgrade;

import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

/*
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.OldField;
import org.apache.arrow.flatbuf.OldSchema;
import org.apache.arrow.flatbuf.Type;
*/
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.Version;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


import io.protostuff.ByteString;

/**
 * To upgrade Arrow Binary Schema to latest Arrow release Dremio uses as of 2.1.0 release
 * Looks like we have 3 stores that store DatasetConfig that contains binary Schema
 */
public class DatasetConfigUpgrade extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "18df9bdf-9186-4780-b6bb-91bcb14a7a8b";

  public DatasetConfigUpgrade() {
    super("Upgrade Arrow Schema", ImmutableList.of());
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_300;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final LegacyKVStoreProvider localStore = context.getKVStoreProvider();

    final LegacyKVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> vdsVersionStore =
      localStore.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    final LegacyIndexedStore<String, NameSpaceContainer> namespaceStore =
      localStore.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);

    Iterable<Map.Entry<String, NameSpaceContainer>> nameSpaces = namespaceStore.find();
    StreamSupport.stream(nameSpaces.spliterator(), false)
      .filter(entry -> NameSpaceContainer.Type.DATASET == entry.getValue().getType())
      .forEach(entry -> {
        DatasetConfig datasetConfig = update(entry.getValue().getDataset());
        if (datasetConfig != null) {
          entry.getValue().setDataset(datasetConfig);
          namespaceStore.put(entry.getKey(), entry.getValue());
        }
      });

    Iterable<Map.Entry<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>> vdsEntries =
      vdsVersionStore.find();

    for (Map.Entry<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> vdsEntry : vdsEntries) {
      VirtualDatasetVersion vdv = vdsEntry.getValue();
      DatasetConfig datasetConfig = update(vdv.getDataset());
      if (datasetConfig == null) {
        continue;
      }
      vdv.setDataset(datasetConfig);
      vdsVersionStore.put(vdsEntry.getKey(), vdv);
    }
  }

  private DatasetConfig update(DatasetConfig datasetConfig) {
    if (datasetConfig == null) {
      return null;
    }
    final io.protostuff.ByteString schemaBytes = DatasetHelper.getSchemaBytes(datasetConfig);
    if (schemaBytes == null) {
      return null;
    }
    return null;
  }


  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
