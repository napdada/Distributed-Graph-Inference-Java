package model;

import ai.djl.Model;
import ai.djl.inference.Predictor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

import static config.Constants.*;

/**
 * Pytorch Encoder 模型定义
 *
 * @author napdada
 * @version : v 0.1 2021/11/15 4:32 下午
 */
@Getter
@Setter
public class Encoder {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(Encoder.class);
    /**
     * Pytorch 模型保存路径
     */
    private String modelPath;
    /**
     * Pytorch 模型名称
     */
    private String modelName;
    /**
     * Pytorch encoder 模型
     */
    private Model model;
    /**
     * 模型输入 Translator
     */
    EncoderTranslator translator;
    /**
     * 模型预测
     */
    Predictor<EncoderInput, EncoderOutput> predictor;

    private volatile static Encoder encoder;

    private Encoder() {
        modelPath = MODEL_PATH;
        modelName = ENCODER_NAME;
        model = Model.newInstance(modelName);
        Path modelDir = Paths.get(modelPath);
        try {
            model.load(modelDir);
            translator = new EncoderTranslator();
            predictor = model.newPredictor(translator);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Encoder getInstance() {
        if (encoder == null) {
            synchronized (Encoder.class) {
                if (encoder == null) {
                    encoder = new Encoder();
                }
            }
        }
        return encoder;
    }

    /**
     * 模型推理
     * @param encoderInput 模型输入
     * @return EncoderOutput 模型输出
     */
    public EncoderOutput infer(EncoderInput encoderInput) {
        try {
            return predictor.predict(encoderInput);
        } catch (Exception e) {
            logger.error("Encoder infer(): " + e.getMessage());
            return null;
        }
    }

    /**
     * 输出模型基本信息
     */
    public void print() {
        logger.info(String.valueOf(model));
    }
}
