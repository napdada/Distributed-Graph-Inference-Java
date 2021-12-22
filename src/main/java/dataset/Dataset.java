package dataset;

import absfunc.edge.*;
import absfunc.triplet.*;
import absfunc.vertex.*;
import config.Constants;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.*;

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
public class Dataset implements Serializable {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(Dataset.class);
    /**
     * 数据集存储路径
     */
    private String path;
    /**
     * JavaSparkContext
     */
    private JavaSparkContext sc;
    /**
     * 数据集构的图
     */
    private Graph<Vdata, Edata> graph;

    public Dataset(JavaSparkContext sc) {
        this.sc = sc;
    }

    public Dataset(String path, JavaSparkContext sc) {
        this.path = path;
        this.sc = sc;
    }

    /**
     * 将事件转换成边 List
     * @param event 事件
     * @return ArrayList<Edge<Edata>> 边 List
     */
    public ArrayList<Edge<Edata>> eventToEdge(String[] event) {
        long srcID = Long.parseLong(event[Constants.SRC_ID_INDEX]);
        long dstID = Long.parseLong(event[Constants.DST_ID_INDEX]);
        float timestamp = Float.parseFloat(event[Constants.TIMESTAMP_INDEX]);
        int label = Integer.parseInt(event[Constants.LABEL_INDEX]);
        float[] feat = new float[Constants.FEATURE_DIM];
        for (int i = 0; i < Constants.FEATURE_DIM; i++) {
            feat[i] = Float.parseFloat(event[i +Constants.FEATURE_INDEX]);
        }
        Edata edata = new Edata(feat, label, timestamp);
        ArrayList<Edge<Edata>> edgeList = new ArrayList<>();
        edgeList.add(new Edge<>(srcID, dstID, edata));
        // 无向图需要双向边
        if (!Constants.HAVE_DIRECTION){
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
            graph.unpersist(false);
            graph = Graph.apply(vRDD, eRDD, new Vdata(), Constants.STORAGE_LEVEL, Constants.STORAGE_LEVEL,
                    Constants.VDATA_CLASS_TAG, Constants.EDATA_CLASS_TAG)
                    .partitionBy(Constants.EDGE_PARTITION2D);
        } else {
            graph = Graph.fromEdges(eRDD, new Vdata(), Constants.STORAGE_LEVEL, Constants.STORAGE_LEVEL,
                    Constants.VDATA_CLASS_TAG, Constants.EDATA_CLASS_TAG)
                    .partitionBy(Constants.EDGE_PARTITION2D);
        }
    }

    /**
     * 从 CSV 中读取数据并构图
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
            creatGraph(null, sc.parallelize(edgeList).rdd());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void genNeighbor() {
        String vertex = "vertex";

        // 1. 第一轮发消息更新一度 subgraph2D set、subgraph2DFeat map
        VertexRDD<Vdata> vRDD = graph.aggregateMessages(new SendVdata(1, vertex), new MergeVdata(vertex),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(1, vertex),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());

        // 2. 第二轮发消息更新二度 subgraph2D set、subgraph2DFeat map
        vRDD = graph.aggregateMessages(new SendVdata(2, vertex), new MergeVdata(vertex),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(2, vertex),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());
    }

    public void encoder(Long src, Long dst) {
        String event = "event", embedding = "embedding";
        // 1. 合并 src、dst 的 subgraph2D set、subgraph2DFeat map 为 eventSubgraph2D set、eventSubgraph2DFeat map
        VertexRDD<Vdata> vRDD = graph.aggregateMessages(new SendVdata(0, event, src, dst), new MergeVdata(0, event),
                TripletFields.All, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(0, event),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());

        // 2. 将 src、dst 的 eventSubgraph2D set 发送给一度点
        vRDD = graph.aggregateMessages(new SendVdata(1, event, src, dst), new MergeVdata(1, event),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(1, event),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());

        // 3. 将 src、dst 的 eventSubgraph2D set 发送给二度点
        vRDD = graph.aggregateMessages(new SendVdata(2, event, src, dst), new MergeVdata(2, event),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(2, event),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());

        // 4. 对 src、dst 进行推理，更新其 embedding map
        graph = graph.mapVertices(new UpdateFeat(src), Constants.VDATA_CLASS_TAG, tpEquals());
        vRDD = graph.aggregateMessages(new SendVdata(0, embedding, src, dst), new MergeVdata(embedding),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(0, embedding),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());

        // 5. 将 embedding 发送给 src、dst 的一度点，并更新点 feat
        vRDD = graph.aggregateMessages(new SendVdata(1, embedding, src, dst), new MergeVdata(embedding),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(1, embedding),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());

        // 6. 将 embedding 发送给 src、dst 的二度点，并更新点 feat
        vRDD = graph.aggregateMessages(new SendVdata(2, embedding, src, dst), new MergeVdata(embedding),
                TripletFields.Src, Constants.VDATA_CLASS_TAG);
        graph = graph.outerJoinVertices(vRDD, new UpdateSubgraph(2, embedding),
                Constants.VDATA_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());
    }

    public void decoder(float timestamp) {
        graph = graph.mapTriplets(new UpdateTriplet(timestamp), Constants.EDATA_CLASS_TAG);
    }

    public int evaluate() {
        EdgeRDD<Integer> edge = graph.edges().mapValues(new Acc(), Constants.INTEGER_CLASS_TAG);
        RDD<Edge<Integer>> negEdge = edge.filter(new NegEdge());
        negEdge.cache();
        negEdge.checkpoint();
        int n = (int) negEdge.count();
        return n;
    }

    public int evaluate(float timestamp) {
        graph = graph.mapEdges(new UpdateAcc(timestamp), Constants.EDATA_CLASS_TAG);
        List<Edge<Edata>> list = graph.edges().filter(new FilterByTs(timestamp)).toJavaRDD().collect();
        return list.size();
    }

    /**
     * 对全图 graph 中同 src、dst 边进行合并（选最新的边）
     */
    public void mergeEdges() {
        Graph<Vdata, Edata> newGraph = graph.groupEdges(new MergeEdge());
        graph.unpersist(false);
        graph = newGraph;
    }

    /**
     * 更新全图点的 timestamp 为最新相关事件的 timestamp
     */
    public void updateTimestamp(Long src, Long dst, float timestamp) {
        Graph<Vdata, Edata> oldGraph = graph;
        graph = graph.mapVertices(new UpdateTime(src, dst, timestamp), Constants.VDATA_CLASS_TAG, tpEquals());
        graph.cache();
        oldGraph.unpersist(false);
    }

    /**
     * 沿着 subgraph 的每条边给 dst 发送 Mail 并 merge 求平均
     * @return VertexRDD<Mail> 求完平均后的 VertexRDD
     */
    public VertexRDD<Mail> getEdgeMsg() {
        VertexRDD<Mail> vertexRDD = graph.aggregateMessages(new SendMail(), new MergeMail(),
                TripletFields.All, Constants.MAIL_CLASS_TAG);
        return vertexRDD.mapValues(new AvgMail(), Constants.MAIL_CLASS_TAG);
    }

    /**
     * 更新二度子图的点 mailbox
     */
    public void updateMailbox() {
        // getEdgeMsg() 沿着 subgraph 的每条边给 dst 发送 Mail 并 merge 求平均，outerJoinVertices() 更新点 mailbox
        graph = graph.outerJoinVertices(getEdgeMsg(), new UpdateMailbox(),
                Constants.MAIL_CLASS_TAG, Constants.VDATA_CLASS_TAG, tpEquals());
    }

    /**
     * 将 Pytorch 模型推理后更新的点特征进行存储
     * @param writerRes BufferedWriter
     */
    public void saveVertexFeat(BufferedWriter writerRes) {
        List<Tuple2<Object, Vdata>> vertexList = graph.vertices().toJavaRDD().collect();
        try {
            writerRes.write(Constants.RESULT_TITLE);
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
            logger.info(e.getMessage());
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
