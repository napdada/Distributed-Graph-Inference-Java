package model;

import ai.djl.Model;
import ai.djl.inference.Predictor;
import config.Constants;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

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

    private static Encoder encoder = new Encoder();

    private Encoder() {
        modelPath = Constants.MODEL_PATH;
        modelName = Constants.ENCODER_NAME;
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
            logger.error(e.getMessage());
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
