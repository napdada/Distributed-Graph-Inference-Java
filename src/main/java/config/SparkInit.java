package config;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

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
        appName = Constants.SPARK_APP_NAME;
        master = Constants.SPARK_MASTER;
        sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setCheckpointDir(Constants.CHECKPOINT_PATH);
    }

    public void unpersistAll(int num) {
        Map<Integer, JavaRDD<?>> map = sparkContext.getPersistentRDDs();
        for (JavaRDD<?> rdd : map.values()) {
            if (rdd.name() == null || !rdd.name().contains(num + Constants.RDD_NAME)) {
                rdd.unpersist();
            }
        }
    }
}
