package dataset;

import com.alibaba.fastjson.JSONArray;
import config.Constants;
import lombok.Getter;
import lombok.Setter;
import model.Encoder;
import model.InputData;
import model.OutputData;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * 节点属性定义
 *
 * @author napdada
 * @version : v 0.1 2021/11/2 5:39 下午
 */
@Getter
@Setter
public class Vdata implements Serializable {
    private Long id;
    /**
     * 节点特征维度（eg. wikipedia 172 维）
     */
    private int featDim = Constants.FEATURE_DIM;
    /**
     * 节点特征
     */
    private float[] feat;
    /**
     * 节点 mailbox
     */
    private ArrayList<Mail> mailbox;
    /**
     * 节点最近更新时间
     */
    private float lastUpdate;
    /**
     * 新发生事件的最大时间戳
     */
    private float timestamp;
    /**
     * 当前点的两度邻居点和边
     */
    private HashSet<String> subgraph2D;
    /**
     * 当前点的两度邻居点特征
     */
    private HashMap<Long, Vfeat> subgraph2DFeat;
    /**
     * 新发生事件的两度邻居点和边
     */
    private HashSet<String> eventSubgraph2D;
    /**
     * 新发生事件的两度点特征
     */
    private HashMap<Long, Vfeat> eventSubgraph2DFeat;
    /**
     * 模型输出 embedding
     */
    private HashMap<Long, float[]> embedding;

    public Vdata() {
        this.feat = new float[featDim];
        this.mailbox = new ArrayList<>();
        this.lastUpdate = 0L;
        this.subgraph2D = new HashSet<>();
        this.subgraph2DFeat = new HashMap<>();
        this.eventSubgraph2D = new HashSet<>();
        this.eventSubgraph2DFeat = new HashMap<>();
        this.embedding = new HashMap<>();
    }

    public Vdata(Long id, Vdata vdata, HashSet<String> subgraph2D, HashMap<Long, Vfeat> subgraph2DFeat,
                 HashSet<String> eventSubgraph2D, HashMap<Long, Vfeat> eventSubgraph2DFeat) {
        this.id = id;
        this.feat = vdata.getFeat();
        this.mailbox = vdata.getMailbox();
        this.lastUpdate = vdata.getLastUpdate();
        this.timestamp = vdata.getTimestamp();
        this.subgraph2D = subgraph2D == null ? new HashSet<>() : subgraph2D;
        this.subgraph2DFeat = subgraph2DFeat == null ? new HashMap<>() : subgraph2DFeat;
        this.eventSubgraph2D = eventSubgraph2D == null ? new HashSet<>() : eventSubgraph2D;
        this.eventSubgraph2DFeat = eventSubgraph2DFeat == null ? new HashMap<>() : eventSubgraph2DFeat;
        this.embedding = vdata.getEmbedding();
    }

    public Vdata(HashSet<String> subgraph2D, HashMap<Long, Vfeat> subgraph2DFeat) {
        this.subgraph2D = subgraph2D == null ? new HashSet<>() : subgraph2D;
        this.subgraph2DFeat = subgraph2DFeat == null ? new HashMap<>() : subgraph2DFeat;
        this.eventSubgraph2D = new HashSet<>();
        this.eventSubgraph2DFeat = new HashMap<>();
    }

    public Vdata(HashSet<String> eventSubgraph2D) {
        this.eventSubgraph2D = eventSubgraph2D;
    }

    public Vdata(HashMap<Long, float[]> embedding) {
        this.embedding = embedding;
    }

    public Vdata(Long id, Vdata vdata, HashMap<Long, float[]> embedding) {
        this.id = id;
        this.featDim = vdata.getFeatDim();
        this.feat = vdata.getFeat();
        this.mailbox = vdata.getMailbox();
        this.lastUpdate = vdata.getLastUpdate();
        this.timestamp = vdata.getTimestamp();
        this.subgraph2D = vdata.getSubgraph2D();
        this.subgraph2DFeat = vdata.getSubgraph2DFeat();
        this.eventSubgraph2D = vdata.getEventSubgraph2D();
        this.eventSubgraph2DFeat = vdata.getEventSubgraph2DFeat();
        this.embedding = embedding;
    }

    public Vdata(Long id, Vdata vdata, float[] feat, HashMap<Long, float[]> embedding) {
        this.id = id;
        this.featDim = vdata.getFeatDim();
        this.feat = feat;
        this.mailbox = vdata.getMailbox();
        this.lastUpdate = vdata.getLastUpdate();
        this.timestamp = vdata.getTimestamp();
        this.subgraph2D = vdata.getSubgraph2D();
        this.subgraph2DFeat = vdata.getSubgraph2DFeat();
        this.eventSubgraph2D = vdata.getEventSubgraph2D();
        this.eventSubgraph2DFeat = vdata.getEventSubgraph2DFeat();
        this.embedding = embedding;
    }

    public Vdata(Long id, float[] feat, ArrayList<Mail> mailbox, float lastUpdate, float timestamp) {
        this.id = id;
        this.feat = feat;
        this.mailbox = mailbox;
        this.lastUpdate = lastUpdate;
        this.timestamp = timestamp;
        this.subgraph2D = new HashSet<>();
        this.subgraph2DFeat = new HashMap<>();
        this.eventSubgraph2D = new HashSet<>();
        this.eventSubgraph2DFeat = new HashMap<>();
        this.embedding = new HashMap<>();
    }

    /**
     *  mailbox 中 mail数量不足上限时用 0 补齐（初始时 mailbox 为空，需要补齐）
     */
    public void alignMailbox() {
        while (mailbox.size() < Constants.MAILBOX_LEN) {
            mailbox.add(new Mail());
        }
    }

    public InputData subGraphToInputdata() {
        int vNum = eventSubgraph2DFeat.size(), i = 0;
        float[][] feat = new float[vNum][];
        float[][][] mail = new float[vNum][][];
        float[] lastUpdate = new float[vNum], timestamp = new float[vNum];

        for (Map.Entry<Long, Vfeat> entry : eventSubgraph2DFeat.entrySet()) {
            feat[i] = entry.getValue().getFeat();
            mail[i] = entry.getValue().mailboxToArray();
            lastUpdate[i] = entry.getValue().getLastUpdate();
            timestamp[i] = entry.getValue().getTimestamp();
            i++;
        }
        return new InputData(feat, mail, lastUpdate, timestamp);
    }

    /**
     * 将 feat[] 转为 String
     * @return String
     */
    public String featToString(float[] featArray) {
        float[] array = featArray == null ? feat : featArray;
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; ; i++) {
            sb.append(array[i]);
            if (i == array.length - 1){
                return sb.append(']').toString();
            }
            sb.append(" ");
        }
    }

    /**
     * 将 mailbox 转为 String
     * @return String
     */
    public String mailboxToString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        alignMailbox();
        for (Mail mail : mailbox) {
            sb.append(featToString(mail.getFeat()));
        }
        return sb.append(']').toString();
    }

    /**
     * 将 mailbox 转为 JSONArray
     * @return JSONArray
     */
    public JSONArray mailboxToJSON() {
        JSONArray mailJSONArray = new JSONArray();
        alignMailbox();
        for (Mail mail : mailbox) {
            mailJSONArray.add(mail.getFeat());
        }
        return mailJSONArray;
    }

    /**
     * 将 mailbox 转为 Array
     * @return float[][]
     */
    public float[][] mailboxToArray() {
        int i = 0;
        float[][] mails = new float[Constants.MAILBOX_LEN][];
        alignMailbox();
        for (Mail mail : mailbox) {
            mails[i++] = mail.getFeat();
        }
        return mails;
    }

    @Override
    public String toString() {
        return "Vdata{" +
                "id=" + id +
                ", featDim=" + featDim +
                ", feat=" + Arrays.toString(feat) +
                ", mailbox=" + mailbox +
                ", lastUpdate=" + lastUpdate +
                ", timestamp=" + timestamp +
                ", subgraph2D=" + subgraph2D +
                ", subgraph2DFeat=" + subgraph2DFeat +
                ", eventSubgraph2D=" + eventSubgraph2D +
                ", eventSubgraph2DFeat=" + eventSubgraph2DFeat +
                ", embedding=" + embedding +
                '}';
    }
}
