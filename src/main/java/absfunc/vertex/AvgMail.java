package absfunc.vertex;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import dataset.Mail;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * 对点收到的 Mail 求平均
 *
 * @author napdada
 * @version : v 0.1 2021/11/17 2:55 下午
 */
public class AvgMail extends AbstractFunction1<Mail, Mail> implements Serializable {
    @Override
    public Mail apply(Mail mail) {
        try(NDManager manager = NDManager.newBaseManager()) {
            NDArray featArray = manager.create(mail.getFeat());
            NDArray avgArray = featArray.div(mail.getNum());
            return new Mail(avgArray.toFloatArray());
        }
    }
}
