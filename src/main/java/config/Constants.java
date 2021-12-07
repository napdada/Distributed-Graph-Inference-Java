package config;

import dataset.Edata;
import dataset.Mail;
import dataset.Vdata;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * 配置常量集合
 *
 * @author napdada
 * @version : v 0.1 2021/11/3 2:44 下午
 */
public class Constants {
    /**
     * Spark 应用名
     */
    public static final String SPARK_APP_NAME = "spark";
    /**
     * Spark 集群 URL（eg. "local"、"spark:master7077"）
     */
    public static final String SPARK_MASTER = "local";

    public static final String RESOURCE_PATH = "/Users/panpan/Documents/Code/Java/spark/src/main/resources/";
    /**
     * 数据集存储路径
     */
//    public static final String DATASET_PATH = RESOURCE_PATH + "dataset/test100.csv";
    public static final String DATASET_PATH = RESOURCE_PATH + "dataset/test1000.csv";
//    public static final String DATASET_PATH = RESOURCE_PATH + "dataset/wikipedia.csv";
//    public static final String DATASET_PATH = RESOURCE_PATH + "dataset/reddit.csv";
    /**
     * 点边特征维度
     */
    public static final int FEATURE_DIM = 172;
    /**
     * CSV 数据集中 src id 的索引列
     */
    public static final int SRC_ID_INDEX = 1;
    /**
     * CSV 数据集中 dst id 的索引列
     */
    public static final int DST_ID_INDEX = 2;
    /**
     * CSV 数据集中 timestamp 的索引列
     */
    public static final int TIMESTAMP_INDEX = 3;
    /**
     * CSV 数据集中 label 的索引列
     */
    public static final int LABEL_INDEX = 4;
    /**
     * CSV 数据集中 feature 的索引列
     */
    public static final int FEATURE_INDEX = 5;

    /**
     * Pytorch 模型存储路径
     */
    public static final String MODEL_PATH = RESOURCE_PATH + "model";
    /**
     * Pytorch 模型名称
     */
    public static final String MODEL_NAME = "Encoder_wikipedia_LP.pt";
    /**
     * Pytorch 模型推理结果题头索引
     */
    public static final String RESULT_TITLE = "vertexID, feat, mailbox, lastUpdate, timestamp";
    /**
     * Pytorch 模型推理结果存储路径
     */
    public static final String RESULT_PATH = RESOURCE_PATH + "result/wikipedia.csv";
    /**
     * Encoder 中是否使用 time embedding
     */
    public static final boolean TIME_EMBEDDING = true;
    /**
     * Encoder 中是否使用 position embedding
     */
    public static final boolean POSITION_EMBEDDING = true;

    /**
     * GraphX 中图为有向 or 无向
     */
    public static final boolean HAVE_DIRECTION = false;
    /**
     * GraphX 中点边的存储级别
     */
    public static final StorageLevel STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK();
    /**
     * GraphX 分区策略
     */
    public static final PartitionStrategy EDGE_PARTITION2D = PartitionStrategy.fromString("EdgePartition2D");
    /**
     * GraphX 分区数量
     */
    public static final int PARTITION_NUM = 16;
    /**
     * mailbox 的最大容量
     */
    public static final int MAILBOX_LEN = 10;
    /**
     * subgraph 跳数
     */
    public static final int HOP_NUN = 2;
    /**
     * GraphX 中自定义点属性的 class tag
     */
    public static final ClassTag<Vdata> VDATA_CLASS_TAG = ClassTag$.MODULE$.apply(Vdata.class);
    /**
     * GraphX 中自定义边属性的 class tag
     */
    public static final ClassTag<Edata> EDATA_CLASS_TAG = ClassTag$.MODULE$.apply(Edata.class);
    /**
     * GraphX 中节点 mail 属性的 class tag
     */
    public static final ClassTag<Mail> MAIL_CLASS_TAG = ClassTag$.MODULE$.apply(Mail.class);
    /**
     * Float tag
     */
    public static final ClassTag<Float> FLOAT_CLASS_TAG = ClassTag$.MODULE$.apply(Float.class);
}
