package config;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

import static config.Constants.*;

/**
 * Spark 初始化
 * 1. 所有 Spark 程序从 SparkContext 开始，SparkContext 的初始化需要一个 SparkConf 对象；
 * 2. SparkConf 包含了 Spark 集群配置的各种参数；
 *
 * @author napdada
 * @version : v 0.1 2021/10/29 11:34 上午
 */
@Data
public class SparkInit {
    /**x
     * 应用名
     */
    private String appName;
    /**
     * 集群 URL（eg. "local"、"spark:master7077"）
     */
    private String master;
    /**
     * Spark 集群配置
     */
    private SparkConf sparkConf;
    /**
     * JavaSparkContext
     */
    private JavaSparkContext sparkContext;

    public SparkInit() {
        appName = SPARK_APP_NAME;
        master = SPARK_MASTER;
        sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setCheckpointDir(CHECKPOINT_PATH);
    }

    /**
     * 从内存中释放没有被当前轮次（turn）标记的中间计算 RDD 结果
     * @param turn 迭代轮次
     */
    public void unpersistAll(int turn) {
        Map<Integer, JavaRDD<?>> map = sparkContext.getPersistentRDDs();
        for (JavaRDD<?> rdd : map.values()) {
            if (rdd.name() == null || !rdd.name().contains(turn + RDD_NAME)) {
                rdd.unpersist();
            }
        }
    }
}
