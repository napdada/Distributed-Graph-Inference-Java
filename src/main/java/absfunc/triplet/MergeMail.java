package absfunc.triplet;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import dataset.Mail;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * GraphX.aggregateMessages() 参数中 mergeMsg 的实现，将每个 dst 节点收到的 feat msg  及 msg 个数进行合并
 *
 * @author napdada
 * @version : v 0.1 2021/11/16 9:24 下午
 */
public class MergeMail extends AbstractFunction2<Mail, Mail, Mail> implements Serializable {
    /**
     * 将点收到的 mail 及 mail 个数进行累加
     * @note merge() 时无法实现求 mail 平均，因此要统计 mail 个数
     * @param mail1 msg1
     * @param mail2 msg2
     * @return Mail(num1 + num2, msg1 + msg2)
     */
    @Override
    public Mail apply(Mail mail1, Mail mail2) {
        try(NDManager manager = NDManager.newBaseManager()) {
            int sum = mail1.getNum() + mail2.getNum();
            NDArray array1 = manager.create(mail1.getFeat());
            NDArray array2 = manager.create(mail2.getFeat());
            NDArray addArray = array1.add(array2);
            return new Mail(sum, addArray.toFloatArray());
        }
    }
}
