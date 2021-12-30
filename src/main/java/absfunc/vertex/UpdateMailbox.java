package absfunc.vertex;

import config.Constants;
import dataset.Mail;
import dataset.Vdata;
import scala.Option;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Graph.outerJoinVertices 参数中 mapFunc 的实现
 * 将 vertex 收到的 mail 加入其 mailbox 中
 * @author napdada
 * @version : v 0.1 2021/11/24 2:34 下午
 */
public class UpdateMailbox extends AbstractFunction3<Object, Vdata, Option<Mail>, Vdata> implements Serializable {
    @Override
    public Vdata apply(Object vID, Vdata v, Option<Mail> newV) {
        if (!newV.isEmpty()) {
            // 注意 new ArrayList<>，否则因为点初始化的问题，所有点的 mailbox 都指向一个引用
            ArrayList<Mail> mailbox = new ArrayList<>(v.getMailbox());
            updateMailbox(mailbox, newV.get());
            return new Vdata((Long) vID, v.getFeat(), mailbox, v.getLastUpdate(), v.getTimestamp());
        }
        return v;
    }

    /**
     * 更新 mailbox，超过上限时会移除最先收到（index = 9）的 mail（前提条件：一个事件发送后，每个点最多收到一个 mail）
     * @param mailbox 点 mailbox
     * @param newMail 新来的 mail
     */
    public void updateMailbox(List<Mail> mailbox, Mail newMail) {
        if (mailbox.size() >= Constants.MAILBOX_LEN) {
            mailbox.remove(Constants.MAILBOX_LEN - 1);
        }
        mailbox.add(0, newMail);
    }
}
