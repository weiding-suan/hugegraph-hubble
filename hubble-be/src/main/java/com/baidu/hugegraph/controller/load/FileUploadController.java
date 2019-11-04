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

import static com.baidu.hugegraph.common.Constant.UPLOAD_PERCENT;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.baidu.hugegraph.common.Constant;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.entity.load.FileMapping;
import com.baidu.hugegraph.entity.load.FileUploadResult;
import com.baidu.hugegraph.exception.ExternalException;
import com.baidu.hugegraph.exception.InternalException;
import com.baidu.hugegraph.options.HubbleOptions;
import com.baidu.hugegraph.service.load.FileMappingService;

import lombok.extern.log4j.Log4j2;

@Log4j2
@RestController
@RequestMapping(Constant.API_VERSION + "graph-connections/{connId}/upload-file")
public class FileUploadController {

    @Autowired
    private HugeConfig config;
    @Autowired
    private FileMappingService fileMappingService;

    @PostMapping("single")
    public FileUploadResult upload(@PathVariable("connId") int connId,
                                   @RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            throw new ExternalException("load.upload.file.cannot-be-empty");
        }

        String fileName = file.getOriginalFilename();
        log.debug("Upload file {} length {}", fileName, file.getSize());
        String location = this.config.get(HubbleOptions.UPLOAD_FILE_LOCATION);
        ensureLocationExist(location);

        FileUploadResult result = new FileUploadResult();
        result.setName(fileName);
        result.setSize(file.getSize());

        StopWatch timer = StopWatch.createStarted();
        String path = Paths.get(location, fileName).toString();
        try (InputStream is = file.getInputStream();
             OutputStream os = new FileOutputStream(new File(path))) {
            IOUtils.copy(is, os);
            // Save file mapping
            FileMapping mapping = new FileMapping(connId, fileName, path);
            int rows = this.fileMappingService.save(mapping);
            if (rows != 1) {
                throw new InternalException("entity.insert.failed", mapping);
            }
            // Get file mapping id
            result.setId(mapping.getId());
            result.setStatus(FileUploadResult.Status.SUCCESS);
        } catch (Exception e) {
            result.setStatus(FileUploadResult.Status.FAILURE);
            result.setCause(e.getMessage());
        } finally {
            timer.stop();
            result.setDuration(timer.getTime(TimeUnit.MILLISECONDS));
        }
        return result;
    }

    // TODO: How to get the progress of each file when uploading multiple files
    @GetMapping("status")
    public Integer uploadStatus(HttpServletRequest request) {
        HttpSession session = request.getSession();
        Object percent = session.getAttribute(UPLOAD_PERCENT);
        return percent != null ? (Integer) percent : 0;
    }

    private static void ensureLocationExist(String location) {
        File locationDir = new File(location);
        if (!locationDir.exists()) {
            try {
                FileUtils.forceMkdir(locationDir);
            } catch (IOException e) {
                throw new InternalException("failed to create location dir", e);
            }
        }
    }
}
