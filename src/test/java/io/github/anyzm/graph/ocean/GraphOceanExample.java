package io.github.anyzm.graph.ocean;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
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
import io.github.anyzm.graph.ocean.domain.VertexQuery;
import io.github.anyzm.graph.ocean.domain.impl.QueryResult;
import io.github.anyzm.graph.ocean.engine.NebulaVertexQuery;
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
public class GraphOceanExample {

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

    private static String space = "first_space";

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
        QueryResult records = nebulaGraphMapper.executeQuerySql("match (n) return n limit 100;");
        System.out.println("records = " + records);
//        User user = new User("UR123", null);
//        user.setUserAge(123);
//        User user2 = new User("UR234", null);
//        user2.setUserAge(123L);
//        保存顶点
//        int i = nebulaGraphMapper.saveVertexEntities(Lists.newArrayList( user2));
//        System.out.println("i = " + i);
        //查询顶点
//        List<User> users = nebulaGraphMapper.fetchVertexTag(User.class, "UR123");
//        System.out.println("users = " + users);
//        保存边和查询边类似
//        Follow follow = new Follow("UR123", "UR234", 1);
//        follow.setUserRankId(123L);
//        System.out.println("follow = " + follow);
        //保存边
//        int i2 = nebulaGraphMapper.saveEdgeEntities(Lists.newArrayList(follow));
//        System.out.println("i2 = " + i2);
//        //查询出边
//        List<Follow> follows = nebulaGraphMapper.goOutEdge(Follow.class, "UR123");
//        System.out.println("follows = " + follows);
//        //查询反向边
//        List<Follow> fans = nebulaGraphMapper.goReverseEdge(Follow.class, "UR123");
//        //查询API
//        VertexQuery queryUserName = NebulaVertexQuery.build().fetchPropOn(User.class, "UR123")
//                .yield(User.class,"userName");
//        QueryResult rows = nebulaGraphMapper.executeQuery(queryUserName);
//        System.out.println(rows);
    }

    @GraphVertex(value = "user", keyPolicy = GraphKeyPolicy.string_key)
    @Data
    public static class User {
        @GraphProperty(value = "user_no", required = true,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_VERTEX_ID, dataType = GraphDataTypeEnum.STRING)
        private String userNo;
        @GraphProperty(value = "user_name", required = true, dataType = GraphDataTypeEnum.STRING)
        private String userName;
        @GraphProperty(value = "user_age", required = true, dataType = GraphDataTypeEnum.INT)
        private Long userAge;
        @GraphProperty(value = "user_rank_id", required = true, dataType = GraphDataTypeEnum.INT, propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_RANK_ID)
        private Long userRankId;


        public User() {
        }

        public User(String userNo, String userName) {
            this.userNo = userNo;
            this.userName = userName;
        }

    }

    @GraphEdge(value = "follow", srcVertex = User.class, dstVertex = User.class, rankIdAsField = false)
    @Data
    public static class Follow {
        @GraphProperty(value = "user_no1", required = false,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_SRC_ID, dataType = GraphDataTypeEnum.STRING)
        private String userNo1;
        @GraphProperty(value = "user_no2", required = false,
                propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_DST_ID, dataType = GraphDataTypeEnum.STRING)
        private String userNo2;
        @GraphProperty(value = "follow_type", required = true, dataType = GraphDataTypeEnum.INT)
        private Long followType;

        @GraphProperty(value = "user_rank_id", required = true, dataType = GraphDataTypeEnum.INT, propertyTypeEnum = GraphPropertyTypeEnum.GRAPH_EDGE_RANK_ID)
        private Long userRankId;
        // rank ：普通模式，只需要一个属性（针对于整形的数据）
        // rank ：redis计算，需要开始节点id、结束节点id、关系、字符串属性[长字符串建议md5使用] （针对于字符串的去重 rank）

        public Follow() {
        }

        public Follow(String userNo1, String userNo2, Long followType) {
            this.userNo1 = userNo1;
            this.userNo2 = userNo2;
            this.followType = followType;
        }
    }

}
