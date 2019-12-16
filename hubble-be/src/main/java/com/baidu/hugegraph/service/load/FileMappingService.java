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

package com.baidu.hugegraph.service.load;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.baidu.hugegraph.entity.load.FileMapping;
import com.baidu.hugegraph.mapper.load.FileMappingMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
public class FileMappingService {

    @Autowired
    private FileMappingMapper mapper;

    public FileMapping get(int id) {
        return this.mapper.selectById(id);
    }

    public List<FileMapping> listAll() {
        return this.mapper.selectList(null);
    }

    public IPage<FileMapping> list(int connId, int pageNo, int pageSize) {
        QueryWrapper<FileMapping> query = Wrappers.query();
        query.eq("conn_id", connId).orderByDesc("name");
        Page<FileMapping> page = new Page<>(pageNo, pageSize);
        return this.mapper.selectPage(page, query);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int save(FileMapping mapping) {
        return this.mapper.insert(mapping);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int update(FileMapping mapping) {
        return this.mapper.updateById(mapping);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int remove(int id) {
        return this.mapper.deleteById(id);
    }
}
