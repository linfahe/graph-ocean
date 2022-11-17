/* Copyright (c) 2022 com.github.anyzm. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
package io.github.anyzm.graph.ocean.engine;

import io.github.anyzm.graph.ocean.common.GraphHelper;
import io.github.anyzm.graph.ocean.common.utils.StringUtil;
import io.github.anyzm.graph.ocean.dao.VertexUpdateEngine;
import io.github.anyzm.graph.ocean.domain.GraphLabel;
import io.github.anyzm.graph.ocean.domain.impl.GraphVertexEntity;
import io.github.anyzm.graph.ocean.domain.impl.GraphVertexType;
import io.github.anyzm.graph.ocean.enums.ErrorEnum;
import io.github.anyzm.graph.ocean.enums.GraphDataTypeEnum;
import io.github.anyzm.graph.ocean.exception.CheckThrower;
import io.github.anyzm.graph.ocean.exception.NebulaException;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 批量顶点更新引擎
 *
 * @author Anyzm
 * date 2020/4/13
 */
public class NebulaBatchVertexUpdate<T> implements VertexUpdateEngine {

    /**
     * nebula> INSERT VERTEX t2 (name, age) VALUES "13":("n3", 12), "14":("n4", 8);
     */
    private static final String VERTEX_UPSET_SQL = "INSERT VERTEX %s ( %s )  VALUES %s:(%s) ";

    private List<GraphVertexEntity<T>> graphVertexEntities;

    private GraphVertexType<T> graphVertexType;

    private int batchSize;


    /**
     * 构建顶点批量插入
     *
     * @param graphVertexEntities
     */
    public NebulaBatchVertexUpdate(List<GraphVertexEntity<T>> graphVertexEntities) throws NebulaException {
        CheckThrower.ifTrueThrow(CollectionUtils.isEmpty(graphVertexEntities), ErrorEnum.UPDATE_FIELD_DATA_NOT_EMPTY);
        this.graphVertexEntities = graphVertexEntities;
        this.graphVertexType = graphVertexEntities.get(0).getGraphVertexType();
        this.batchSize = graphVertexEntities.size();
    }

    private String getOneVertexSql() throws NebulaException {
        return generateUpsetSql(this.graphVertexEntities.get(0));
    }

    private List<String> getMultiVertexSql() throws NebulaException {
        // nebula> UPSERT VERTEX 111 SET player.name = "Dwight Howard", player.age = $^.player.age + 11;
        List<String> sqlList = Lists.newArrayListWithExpectedSize(batchSize);
        for (GraphVertexEntity graphVertexEntity : this.graphVertexEntities) {
            String sql = generateUpsetSql(graphVertexEntity);
            sqlList.add(sql);
        }
        return StringUtil.aggregate(sqlList, batchSize, ";");
    }

    private String generateUpsetSql(GraphVertexEntity graphVertexEntity) throws NebulaException {
        String queryId = GraphHelper.getQueryId(this.graphVertexType, graphVertexEntity.getId());
        StringBuilder builder = new StringBuilder();
        Map<String, GraphDataTypeEnum> dataTypeMap = graphVertexEntity.getGraphVertexType().getDataTypeMap();
        // nebula> INSERT VERTEX t2 (name, age) VALUES "13":("n3", 12), "14":("n4", 8);
        // INSERT VERTEX %s ( %s )  VALUES %s:(%s)
        // 获取 所有的属性
        String fields = graphVertexType.getMustFields().stream().collect(Collectors.joining(","));
        // 拼接 value
        Map<String, Object> entityProps = graphVertexEntity.getProps();
        for (String fieldName : graphVertexType.getMustFields()) {
            GraphDataTypeEnum graphDataTypeEnum = dataTypeMap.get(fieldName);
            if (entityProps.containsKey(fieldName)) {
                if (GraphDataTypeEnum.STRING.equals(graphDataTypeEnum)) {
                    builder.append(',').append(" \"").append(entityProps.get(fieldName)).append("\"");
                } else {
                    builder.append(',').append(entityProps.get(fieldName));
                }
            } else {
                builder.append(",NULL");
            }
        }
        String sqlSet = builder.delete(0, 1).toString();
        return String.format(VERTEX_UPSET_SQL, graphVertexEntity.getGraphVertexType().getVertexName(), fields, queryId, sqlSet);
    }


    @Override
    public List<GraphVertexEntity<T>> getGraphVertexEntityList() {
        return this.graphVertexEntities;
    }

    @Override
    public GraphVertexType<T> getGraphVertexType() {
        return this.graphVertexType;
    }

    @Override
    public List<String> getSqlList() throws NebulaException {
        if (this.batchSize == 1) {
            return Lists.newArrayList(getOneVertexSql());
        }
        return getMultiVertexSql();
    }

    @Override
    public List<GraphLabel> getLabels() {
        return Lists.newArrayList(this.getGraphVertexType());
    }
}
