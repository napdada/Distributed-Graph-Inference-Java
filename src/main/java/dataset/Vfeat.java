package dataset;

import config.Constants;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 点特征类
 * @author napdada
 * @version : v 0.1 2021/11/29 4:23 下午
 */
@Getter
@Setter
public class Vfeat implements Serializable {
    /**
     * 点特征
     */
    private float[] feat;
    /**
     * 点 mailbox
     */
    private ArrayList<Mail> mailbox;
    /**
     * 点最近更新事件
     */
    private float lastUpdate;
    /**
     * 点时间戳
     */
    private float timestamp;

    public Vfeat(Vdata vdata) {
        this.feat = vdata.getFeat();
        this.mailbox = vdata.getMailbox();
        this.lastUpdate = vdata.getLastUpdate();;
        this.timestamp = vdata.getTimestamp();
    }

    /**
     *  mailbox 中 mail数量不足上限时用 0 补齐（初始时 mailbox 为空，需要补齐）
     */
    public void alignMailbox() {
        while (mailbox.size() < Constants.MAILBOX_LEN) {
            mailbox.add(new Mail());
        }
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
}
