/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.controller.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.baidu.hugegraph.common.Constant;
import com.baidu.hugegraph.controller.BaseController;
import com.baidu.hugegraph.entity.load.AdvancedSetting;
import com.baidu.hugegraph.entity.load.EdgeMapping;
import com.baidu.hugegraph.entity.load.FileMapping;
import com.baidu.hugegraph.entity.load.FileSummary;
import com.baidu.hugegraph.entity.load.HeaderSetting;
import com.baidu.hugegraph.entity.load.LoadParameter;
import com.baidu.hugegraph.entity.load.VertexMapping;
import com.baidu.hugegraph.exception.ExternalException;
import com.baidu.hugegraph.exception.InternalException;
import com.baidu.hugegraph.service.load.FileMappingService;
import com.baomidou.mybatisplus.core.metadata.IPage;

import lombok.extern.log4j.Log4j2;

@Log4j2
@RestController
@RequestMapping(Constant.API_VERSION + "graph-connections/{connId}/load")
public class FileMappingController extends BaseController {

    @Autowired
    private FileMappingService service;

    @GetMapping("file-summaries/{id}")
    public FileSummary getSummary(@PathVariable("id") int id) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        File file = FileUtils.getFile(mapping.getPath());
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new InternalException("The file '%s' is not found", file);
        }
        String delimiter = mapping.getAdvancedSetting().getDelimiter();

        String[] columnNames;
        String[] columnValues;
        try {
            String line = reader.readLine();
            String[] firstLine = StringUtils.split(line, delimiter);
            if (mapping.isHasHeader()) {
                columnNames = firstLine;
                line = reader.readLine();
                columnValues = StringUtils.split(line, delimiter);
            } else {
                columnValues = firstLine;
                columnNames = new String[columnValues.length];
                for (int i = 1; i <= columnValues.length; i++) {
                    columnNames[i] = "column-" + i;
                }
            }
        } catch (IOException e) {
            throw new InternalException("Failed to read header and sample " +
                                        "data from file '%s'", file);
        } finally {
            try {
                reader.close();
            } catch (IOException ignored) {
                log.warn("Failed to close reader for file {}", file);
            }
        }
        // Read file
        return new FileSummary(columnNames, columnValues);
    }

    @GetMapping("file-mappings")
    public IPage<FileMapping> list(@PathVariable("connId") int connId,
                                   @RequestParam(name = "page_no",
                                                 required = false,
                                                 defaultValue = "1")
                                   int pageNo,
                                   @RequestParam(name = "page_size",
                                                 required = false,
                                                 defaultValue = "10")
                                   int pageSize) {
        return this.service.list(connId, pageNo, pageSize);
    }

    @DeleteMapping("file-mappings/{id}")
    public void delete(@PathVariable("id") int id) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        int rows = this.service.remove(id);
        if (rows != 1) {
            throw new InternalException("entity.delete.failed", mapping);
        }
    }

    @PostMapping("file-mappings/{id}/header-setting")
    public void headerSetting(@PathVariable("id") int id,
                              @RequestBody HeaderSetting newEntity) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        mapping.setHasHeader(newEntity.isHasHeader());
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }

    @PostMapping("file-mappings/{id}/advanced-setting")
    public void advancedSetting(@PathVariable("id") int id,
                                @RequestBody AdvancedSetting newEntity) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        AdvancedSetting oldEntity = mapping.getAdvancedSetting();
        AdvancedSetting entity = this.mergeEntity(oldEntity, newEntity);
        mapping.setAdvancedSetting(entity);
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }

    @PostMapping("file-mappings/{id}/vertex-mappings")
    public void addVertexMapping(@PathVariable("id") int id,
                                 @RequestBody VertexMapping newEntity) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        mapping.getVertexMappings().put(newEntity.getLabel(), newEntity);
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }

    @DeleteMapping("file-mappings/{id}/vertex-mappings/{label}")
    public void deleteVertexMapping(@PathVariable("id") int id,
                                    @PathVariable("label") String label) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        VertexMapping oldEntity = mapping.getVertexMappings().remove(label);
        if (oldEntity == null) {
            throw new ExternalException(
                      "load.file-mapping.vertex-mapping.not-exist.label", label);
        }
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }

    @PostMapping("file-mappings/{id}/edge-mappings")
    public void addEdgeMapping(@PathVariable("id") int id,
                               @RequestBody EdgeMapping newEntity) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        mapping.getEdgeMappings().put(newEntity.getLabel(), newEntity);
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }

    @DeleteMapping("file-mappings/{id}/edge-mappings/{label}")
    public void deleteEdgeMapping(@PathVariable("id") int id,
                                  @PathVariable("label") String label) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        EdgeMapping oldEntity = mapping.getEdgeMappings().remove(label);
        if (oldEntity == null) {
            throw new ExternalException(
                      "load.file-mapping.edge-mapping.not-exist.label", label);
        }
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }

    @PostMapping("file-mappings/{id}/load-parameter")
    public void loadParameter(@PathVariable("id") int id,
                              @RequestBody LoadParameter newEntity) {
        FileMapping mapping = this.service.get(id);
        if (mapping == null) {
            throw new ExternalException("load.file-mapping.not-exist.id", id);
        }

        LoadParameter oldEntity = mapping.getLoadParameter();
        LoadParameter entity = this.mergeEntity(oldEntity, newEntity);
        mapping.setLoadParameter(entity);
        int rows = this.service.update(mapping);
        if (rows != 1) {
            throw new InternalException("entity.update.failed", mapping);
        }
    }
}
