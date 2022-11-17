package io.github.anyzm.graph.ocean;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import io.github.anyzm.graph.ocean.annotation.GraphEdge;
import io.github.anyzm.graph.ocean.annotation.GraphProperty;
import io.github.anyzm.graph.ocean.annotation.GraphVertex;
import io.github.anyzm.graph.ocean.domain.impl.QueryResult;
import io.github.anyzm.graph.ocean.enums.GraphDataTypeEnum;
import io.github.anyzm.graph.ocean.enums.GraphKeyPolicy;
import io.github.anyzm.graph.ocean.enums.GraphPropertyTypeEnum;
import io.github.anyzm.graph.ocean.mapper.NebulaGraphMapper;
import io.github.anyzm.graph.ocean.session.NebulaPoolSessionManager;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @Author ZhaoLai Huang
 * created by ZhaoLai Huang on 2022/4/10
 */
public class GraphOceanExample2 {

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        List<ch.qos.logback.classic.Logger> loggerList = loggerContext.getLoggerList();
        loggerList.forEach(logger -> {
            logger.setLevel(Level.DEBUG);
        });
    }

    private static int nebulaPoolMaxConnSize = 1000;

    private static int nebulaPoolMinConnSize = 50;

    private static int nebulaPoolIdleTime = 180000;

    private static int nebulaPoolTimeout = 300000;

    private static String nebulaCluster = "192.168.0.101:9669";

    private static String userName = "root";

    private static String password = "nebula";

    private static String space = "wu_yi";
//    private static String space = "first_space";

    public static NebulaPoolConfig nebulaPoolConfig() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(nebulaPoolMaxConnSize);
        nebulaPoolConfig.setMinConnSize(nebulaPoolMinConnSize);
        nebulaPoolConfig.setIdleTime(nebulaPoolIdleTime);
        nebulaPoolConfig.setTimeout(nebulaPoolTimeout);
        return nebulaPoolConfig;
    }

    public static NebulaPool nebulaPool(NebulaPoolConfig nebulaPoolConfig)
            throws UnknownHostException {
        List<HostAddress> addresses = null;
        try {
            String[] hostPorts = StringUtils.split(nebulaCluster, ",");
            addresses = Lists.newArrayListWithExpectedSize(hostPorts.length);
            for (String hostPort : hostPorts) {
                String[] linkElements = StringUtils.split(hostPort, ":");
                HostAddress hostAddress = new HostAddress(linkElements[0],
                        Integer.valueOf(linkElements[1]));
                addresses.add(hostAddress);
            }
        } catch (Exception e) {
            throw new RuntimeException("nebula数据库连接信息配置有误，正确格式：ip1:port1,ip2:port2");
        }
        NebulaPool pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);
        return pool;
    }

    public static NebulaPoolSessionManager nebulaPoolSessionManager(NebulaPool nebulaPool) {
        return new NebulaPoolSessionManager(nebulaPool, userName, password, true);
    }

    public static NebulaGraphMapper nebulaGraphMapper(
            NebulaPoolSessionManager nebulaPoolSessionManager) {
        return new NebulaGraphMapper(nebulaPoolSessionManager, space);
    }

    public static void main(String[] args) throws UnknownHostException, UnsupportedEncodingException, IllegalAccessException, InstantiationException, ClientServerIncompatibleException, AuthFailedException, NotValidConnectionException, IOErrorException {
        NebulaGraphMapper nebulaGraphMapper = nebulaGraphMapper(nebulaPoolSessionManager(
                nebulaPool(nebulaPoolConfig())));
//        QueryResult records = nebulaGraphMapper.executeQuerySql("match (n) return n limit 100;");
//        QueryResult records = nebulaGraphMapper.executeQuerySql(" MATCH (m)<-[r:in_group]-(n:qq) WHERE id(m)==\"956728735\" RETURN id(n) as qqId , n.qq.nick_name as qqNickName;");
//        System.out.println("records = \n " + records);
        List<QqVo> qqVos = nebulaGraphMapper.executeQuerySql("  MATCH (m)<-[r:in_group]-(n:qq) WHERE id(m)==\"956728735\" RETURN id(n) as qqId , n.qq.nick_name as qqNickName,id(m) as qqGroupId,m.qq_group.name as qqGroupName,r.in_group_remark as inGroupRemark;", QqVo.class);
        System.out.println("qqVos = \n" + JSONObject.toJSONString(qqVos));
        List<Qq> qqs = nebulaGraphMapper.fetchVertexTag(Qq.class, "3419559904");
        System.out.println("qqs = " + qqs);
    }

    @Data
    public static class QqVo {
        private String qqId;
        private String qqNickName;

        private String qqGroupId;
        private String qqGroupName;

        private String inGroupRemark;

    }

    @GraphVertex(value = "qq", keyPolicy = GraphKeyPolicy.string_key)
    @Data
    public static class Qq {
        @GraphProperty(value = "account", required = true,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_VERTEX_ID, dataType = GraphDataTypeEnum.STRING)
        private String account;
        @GraphProperty(value = "name", required = true, dataType = GraphDataTypeEnum.STRING)
        private String name;
        @GraphProperty(value = "nick_name", required = true, dataType = GraphDataTypeEnum.STRING)
        private String nickName;

        public Qq() {
        }

        public Qq(String account, String name, String nickName) {
            this.account = account;
            this.name = name;
            this.nickName = nickName;
        }

    }

    @GraphVertex(value = "qq_group", keyPolicy = GraphKeyPolicy.string_key)
    @Data
    public static class QqGroup {
        @GraphProperty(value = "account", required = true,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_VERTEX_ID, dataType = GraphDataTypeEnum.STRING)
        private String account;
        @GraphProperty(value = "name", required = true, dataType = GraphDataTypeEnum.STRING)
        private String name;
        @GraphProperty(value = "g_desc", required = true, dataType = GraphDataTypeEnum.STRING)
        private String gDesc;

        public QqGroup() {
        }

        public QqGroup(String account, String name, String gDesc) {
            this.account = account;
            this.name = name;
            this.gDesc = gDesc;
        }

    }

    @GraphEdge(value = "in_group", srcVertex = Qq.class, dstVertex = QqGroup.class, rankIdAsField = false
            , srcIdAsField = false, dstIdAsField = false)
    @Data
    public static class InGroup {
        @GraphProperty(value = "src_id", required = false,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_SRC_ID, dataType = GraphDataTypeEnum.STRING)
        private String srcId;
        @GraphProperty(value = "dst_id", required = false,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_DST_ID, dataType = GraphDataTypeEnum.STRING)
        private String dstId;
        @GraphProperty(value = "in_group_flag", required = true, dataType = GraphDataTypeEnum.BOOLEAN)
        private Boolean inGroupFlag;
        @GraphProperty(value = "in_group_remark", required = true, dataType = GraphDataTypeEnum.STRING)
        private String inGroupRemark;

        @GraphProperty(value = "user_rank_id", required = true, dataType = GraphDataTypeEnum.INT, propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_RANK_ID)
        private Long userRankId;
        // rank ：普通模式，只需要一个属性（针对于整形的数据）
        // rank ：redis计算，需要开始节点id、结束节点id、关系、字符串属性[长字符串建议md5使用] （针对于字符串的去重 rank）

        public InGroup() {
        }

        public InGroup(String srcId, String dstId, Boolean inGroupFlag, String inGroupRemark, Long userRankId) {
            this.srcId = srcId;
            this.dstId = dstId;
            this.inGroupFlag = inGroupFlag;
            this.inGroupRemark = inGroupRemark;
            this.userRankId = userRankId;
        }
    }

}
