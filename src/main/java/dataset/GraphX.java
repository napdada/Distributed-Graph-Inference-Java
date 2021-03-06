package dataset;

import absfunc.edge.NegFilter;
import absfunc.edge.MergeEdge;
import absfunc.edge.UpdateRes;
import absfunc.triplet.*;
import absfunc.vertex.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static config.Constants.*;

/**
 * 图数据集（csv 格式）
 * eg.
 *      index | src_id | dst_id | timestamp | label | feature
 *      0       0        1        1           0       [-0.1, 0.2, ...]
 *      1       0        2        36          0       [-0.1, 0.3, ...]
 *
 * @author napdada
 * @version : v 0.1 2021/10/29 11:09 上午
 */
@Getter
@Setter
public class GraphX implements Serializable {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(GraphX.class);
    /**
     * 数据集存储路径
     */
    private String path;
    /**
     * 数据集构的图
     */
    private Graph<Vdata, Edata> graph;

    public GraphX() {
        this.path = RESULT_PATH;
    }

    /**
     * Test.java 使用，从 CSV 中读取数据并构图
     */
    public void readData() {
        File csv = new File(this.path);
        try {
            // 1. 读取数据集中的边
            BufferedReader bufferedReader = new BufferedReader(new FileReader(csv));
            String lineData = "";
            ArrayList<Edge<Edata>> edgeList = new ArrayList<>();
            lineData = bufferedReader.readLine();
            while ((lineData = bufferedReader.readLine()) != null) {
                String[] line = lineData.split(",");
                edgeList.addAll(eventToEdge(line));
            }
            bufferedReader.close();

            // 2. 构图
            creatGraph(null, SC.parallelize(edgeList).rdd());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将事件转换成边 List
     * @param event 事件
     * @return ArrayList<Edge<Edata>> 边 List
     */
    public ArrayList<Edge<Edata>> eventToEdge(String[] event) {
        long srcID = Long.parseLong(event[SRC_ID_INDEX]);
        long dstID = Long.parseLong(event[DST_ID_INDEX]);
        float timestamp = Float.parseFloat(event[TIMESTAMP_INDEX]);
        int label = Integer.parseInt(event[LABEL_INDEX]);
        float[] feat = new float[FEATURE_DIM];
        for (int i = 0; i < FEATURE_DIM; i++) {
            feat[i] = Float.parseFloat(event[i + FEATURE_INDEX]);
        }
        Edata edata = new Edata(feat, label, timestamp);
        ArrayList<Edge<Edata>> edgeList = new ArrayList<>();
        edgeList.add(new Edge<>(srcID, dstID, edata));
        // 无向图需要双向边
        if (!HAVE_DIRECTION){
            edgeList.add(new Edge<>(dstID, srcID, edata));
        }
        return edgeList;
    }

    /**
     * 使用点边 RDD 构图
     * @param vRDD 点 RDD
     * @param eRDD 边 RDD
     */
    public void creatGraph(RDD<Tuple2<Object, Vdata>> vRDD, RDD<Edge<Edata>> eRDD) {
        if (vRDD != null) {
            graph = Graph.apply(vRDD, eRDD, new Vdata(), STORAGE_LEVEL, STORAGE_LEVEL,
                    VDATA_CLASS_TAG, EDATA_CLASS_TAG)
                    .partitionBy(EDGE_PARTITION2D, PARTITION_NUM);
        } else {
            graph = Graph.fromEdges(eRDD, new Vdata(), STORAGE_LEVEL, STORAGE_LEVEL,
                    VDATA_CLASS_TAG, EDATA_CLASS_TAG)
                    .partitionBy(EDGE_PARTITION2D, PARTITION_NUM);
        }
    }

    /**
     * 用于测试基本功能的 demo graph
     * @return Graph<Vdata, Edata>
     */
    public Graph<Vdata, Edata> demoGraph() {
        Edata edata = new Edata();
        ArrayList<Tuple2<Object, Vdata>> v = new ArrayList<>();
        v.add(new Tuple2<>(0L, new Vdata()));
        v.add(new Tuple2<>(1L, new Vdata()));
        v.add(new Tuple2<>(2L, new Vdata()));
        v.add(new Tuple2<>(3L, new Vdata()));
        v.add(new Tuple2<>(4L, new Vdata()));
        v.add(new Tuple2<>(5L, new Vdata()));
        v.add(new Tuple2<>(6L, new Vdata()));
        v.add(new Tuple2<>(7L, new Vdata()));
        v.add(new Tuple2<>(8L, new Vdata()));
        v.add(new Tuple2<>(9L, new Vdata()));
        v.add(new Tuple2<>(10L, new Vdata()));
        v.add(new Tuple2<>(11L, new Vdata()));
        ArrayList<Edge<Edata>> l = new ArrayList<>();
        l.add(new Edge<>(0, 1, edata));l.add(new Edge<>(1, 0, edata));
        l.add(new Edge<>(0, 2, edata));l.add(new Edge<>(2, 0, edata));
        l.add(new Edge<>(0, 3, edata));l.add(new Edge<>(3, 0, edata));
        l.add(new Edge<>(0, 5, edata));l.add(new Edge<>(5, 0, edata));
        l.add(new Edge<>(7, 5, edata));l.add(new Edge<>(5, 7, edata));
        l.add(new Edge<>(4, 1, edata));l.add(new Edge<>(1, 4, edata));
        l.add(new Edge<>(6, 1, edata));l.add(new Edge<>(1, 6, edata));
        l.add(new Edge<>(8, 1, edata));l.add(new Edge<>(1, 8, edata));
        l.add(new Edge<>(8, 9, edata));l.add(new Edge<>(9, 8, edata));
        l.add(new Edge<>(8, 10, edata));l.add(new Edge<>(10, 8, edata));
        l.add(new Edge<>(9, 10, edata));l.add(new Edge<>(10, 9, edata));
        l.add(new Edge<>(11, 9, edata));l.add(new Edge<>(9, 11, edata));
        return Graph.apply(SC.parallelize(v).rdd(), SC.parallelize(l).rdd(), new Vdata(), STORAGE_LEVEL, STORAGE_LEVEL,
                VDATA_CLASS_TAG, EDATA_CLASS_TAG)
                .partitionBy(EDGE_PARTITION2D);
    }

    /**
     * 对全图 graph 中同 src、dst 边进行合并（选最新的边）
     */
    public void mergeEdges() {
        graph = graph.groupEdges(new MergeEdge());
    }

    /**
     * 更新全图点的 timestamp 为最新相关事件的 timestamp
     */
    public void updateTimestamp(Long src, Long dst, float timestamp) {
        graph = graph.mapVertices(new UpdateTime(src, dst, timestamp), VDATA_CLASS_TAG, tpEquals());
    }

    /**
     * 新事件二度子图
     * 1. 利用 pregel 获取新事件的 2DSubgraph（hop = 2、hop = 1、hop = 0），每轮 hop 递减
     * 2. 将 2DSubgraph 中点的 Vfeat 发送回 src、dst，每轮 hop 递增
     * @param src src ID
     * @param dst dst ID
     */
    public void event2DSubgraph(Long src, Long dst) {
        graph = graph.ops().pregel(2, 2, EdgeDirection.Out(),
                new UpdateHop(), new SendHop(src, dst), new MergeHop(), INTEGER_CLASS_TAG);

        graph = graph.ops().pregel(new HashMap<>(), 3, EdgeDirection.Out(),
                new Update2DSubgraph(), new SendVfeat(), new MergeVfeat(), SUBGRAPH_MAP_CLASS_TAG);
    }

    /**
     * 将新事件 event(src, dst) 的二度子图输入 encoder 模型（在 src 上进行推理）获得 embedding
     * 并通过 send embedding msg 方式将 embedding 结果发给二度子图中所有点，并更新 feat
     * @param src srd ID
     * @param dst dst ID
     */
    public void encoder(Long src, Long dst) {
        graph = graph.mapVertices(new UpdateFeat(src), VDATA_CLASS_TAG, tpEquals());
    }

    /**
     * 通过 send embedding msg 方式将 embedding 结果发给二度子图中所有点，并更新 feat
     * @param src srd ID
     * @param dst dst ID
     */
    public void sendEmd(Long src, Long dst) {
        graph = graph.ops().pregel(new HashMap<>(), 3, EdgeDirection.Out(),
                new UpdateEmb(), new SendEmb(src, dst), new MergeEmb(), EMBEDDING_MAP_CLASS_TAG);
    }

    /**
     * 更新二度子图的点 mailbox，先 send mail 再求平均
     */
    public void updateMailbox() {
        VertexRDD<Mail> vRDD = graph.aggregateMessages(new SendMail(), new MergeMail(),
                TripletFields.All, MAIL_CLASS_TAG);
        vRDD = vRDD.mapValues(new AvgMail(), MAIL_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateMailbox(),
                MAIL_CLASS_TAG, VDATA_CLASS_TAG, tpEquals());
    }

    /**
     * 将新事件 event(src, dst) 的 embedding 进行解码（输入到 decoder）
     * 获得 logits、labels 并更新边 acc
     * @param src src ID
     * @param dst dst ID
     */
    public void decoder(Long src, Long dst) {
        graph = graph.mapTriplets(new UpdateRes(src, dst), EDATA_CLASS_TAG);
    }

    /**
     * 评估图推理结果，统计 accuracy
     * @param src src ID
     * @param dst dst ID
     * @param num 迭代轮次
     * @return accuracy 数
     */
    public int evaluate(Long src, Long dst, int num) {
        RDD<Edge<Edata>> eRDD = graph.edges().filter(new NegFilter(src, dst));
        eRDD.cache();
        graph.cache();
        graph.vertices().setName(num + RDD_NAME);
        graph.edges().setName(num + RDD_NAME);
        return (int) eRDD.count();
    }

    /**
     * 将 Pytorch 模型推理后更新的点特征进行存储
     */
    public void saveVertexFeat() throws IOException {
        File resCsv = new File(RESULT_PATH);
        BufferedWriter writerRes = new BufferedWriter(new FileWriter(resCsv));
        List<Tuple2<Object, Vdata>> vertexList = graph.vertices().toJavaRDD().collect();
        try {
            writerRes.write(RESULT_TITLE);
            for (Tuple2<Object, Vdata> v : vertexList) {
                writerRes.newLine();
                writerRes.write(v._1 + ",");
                writerRes.write(v._2.featToString(null) + ",");
                writerRes.write(v._2.mailboxToString() + ",");
                writerRes.write(v._2.getLastUpdate() + ",");
                writerRes.write(String.valueOf(v._2.getTimestamp()));
            }
            // 使用缓冲区的刷新方式将数据刷到目的地
            writerRes.flush();
            writerRes.close();
        } catch (Exception e) {
            logger.error("GraphX saveVertexFeat(): " + e.getMessage());
        }
    }

    /**
     * 更新前后的点边属性相同
     * @param <T> 点边属性
     * @return scala.Predef.$eq$colon$eq$.MODULE$.tpEquals()
     */
    static public <T> scala.Predef.$eq$colon$eq<T, T> tpEquals() {
        return scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
    }

    /**
     * 输出简单图信息（点边数、部分点边详情）
     */
    public void print() {
        logger.info("----------------------------------------");
        logger.info("图节点个数：" + graph.vertices().count() + "，边个数：" + graph.edges().count());
        logger.info("图部分点边内容：");;
        logger.info("----------------------------------------");
    }

    /**
     * 输出 dataset 全图所有点信息
     */
    public void printVertexs() {
        List<Tuple2<Object, Vdata>> vList = graph.vertices().toJavaRDD().collect();
        System.out.println("所有点：");
        for (Tuple2<Object, Vdata> v : vList) {
            System.out.println(v.toString());
        }
    }

    /**
     * 输出 vertexRDD 所有点信息
     * @param vertexRDD 点 RDD
     */
    public void printVertexs(VertexRDD<Vdata> vertexRDD) {
        List<Tuple2<Object, Vdata>> vList = vertexRDD.toJavaRDD().collect();
        logger.info("所有点：");
        for (Tuple2<Object, Vdata> v : vList) {
            logger.info(v.toString());
        }
    }

    /**
     * 输出 graph 所有点信息
     * @param graph 图
     */
    public void printVertexs(Graph<Vdata, Edata> graph) {
        List<Tuple2<Object, Vdata>> vList = graph.vertices().toJavaRDD().collect();
        logger.info("所有点：");
        for (Tuple2<Object, Vdata> v : vList) {
            logger.info(v.toString());
        }
    }

    /**
     * 输出 dataset 全图所有边信息
     */
    public void printEdges() {
        List<Edge<Edata>> eList = graph.edges().toJavaRDD().collect();
        logger.info("所有边：");
        for (Edge<Edata> e : eList) {
            logger.info(e.toString());
        }
    }

    /**
     * 输出 edgeRDD 所有边信息
     * @param edgeRDD 边 RDD
     */
    public void printEdges(EdgeRDD<Edata> edgeRDD) {
        List<Edge<Edata>> eList = edgeRDD.toJavaRDD().collect();
        logger.info("所有边：");
        for (Edge<Edata> e : eList) {
            logger.info(e.toString());
        }
    }

    /**
     * 输出 graph 所有边信息
     * @param graph 图
     */
    public void printEdges(Graph<Vdata, Edata> graph) {
        List<Edge<Edata>> eList = graph.edges().toJavaRDD().collect();
        logger.info("所有边：");
        for (Edge<Edata> e : eList) {
            logger.info(e.toString());
        }
    }


    /**
     * 输出 dataset 全图所有点边信息
     */
    public void printAll() {
        List<Tuple2<Object, Vdata>> vList = graph.vertices().toJavaRDD().collect();
        List<Edge<Edata>> eList = graph.edges().toJavaRDD().collect();
        System.out.println("所有点：");
        for (Tuple2<Object, Vdata> v : vList) {
            System.out.println(v.toString());
        }
        System.out.println("所有边：");
        for (Edge<Edata> e : eList) {
            System.out.println(e.toString());
        }
    }

    /**
     * 输出 graph 所有点边信息
     * @param graph 图
     */
    public void printAll(Graph<Vdata, Edata> graph) {
        List<Tuple2<Object, Vdata>> vList = graph.vertices().toJavaRDD().collect();
        List<Edge<Edata>> eList = graph.edges().toJavaRDD().collect();
        System.out.println("所有点：");
        for (Tuple2<Object, Vdata> v : vList) {
            System.out.println(v.toString());
        }
        System.out.println("所有边：");
        for (Edge<Edata> e : eList) {
            System.out.println(e.toString());
        }
    }
}
