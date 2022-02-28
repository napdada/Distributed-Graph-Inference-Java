# Distributed-Graph-Inference-Java

## 1. Introduction

A graph is a structure that can effectively represent objects and the relationships between them. Graph Neural Networks (GNNs) enable deep learning to be applied in the graph domain. However, most GNN models are trained offline and cannot be directly used in real-time monitoring scenarios. In addition, due to the very large data scale of the graph, a single machine cannot meet the demand, and there is a performance bottleneck. Therefore, we propose a distributed graph neural network inference computing framework, which can be applied to GNN models in the form of Encoder-Decoder. We propose the idea of “single-point inference, message passing, distributed computing”, which enables the system to use offline-trained GNNs for real-time inference computations on graph data. In order to maintain the model effect, we add the second-degree subgraph and mailbox mechanism to the continuous iterative calculation. Finally, our results on public datasets show that this method greatly improves the upper limit of inference computation and has better timeliness. And it maintains a good model effect on three types of classical tasks.

## 2. Method

The framework is mainly composed of the following modules: incremental composition, second-degree subgraph calculation, GNN encoder, mailbox, GNN decoder.

## 3. Environment

| Category            | Details                                              |
| ------------------- | ---------------------------------------------------- |
| processor           | Intel(R) Core(TM) i5-8257U CPU @2.00GHz              |
| memory              | 16GB                                                 |
| OS                  | macOS Catalina 10.15.7                               |
| development tool    | IntelliJ IDEA 2020.1.2                               |
| running environment | Spark 3.2.0, Hadoop 3.3.0, Scala 2.12.15, Java 1.8.0 |

## 4. Run

You can open the project directly in IDEA, configure the environment, and set the running parameters, then run `Main.java` directly.

### 4.1 Configurable parameters

```shell
Execute command parameter order:
command RESOURCE_PATH DATASET_NAME TASK_NAME CHECKPOINT_FREQUENCY MAX_EVENT_NUM

1. RESOURCE_PATH: Static resource(dataset/model/log/checkpoint/result) path
2. DATASET_NAME: Dataset name(Wikipedia or Reddit)
3. TASK_NAME: The name of the task performed(LP/NC/EC)
4. CHECKPOINT_FREQUENCY: Frequency of truncating RDD lineages(1/2/.../10)
5. MAX_EVENT_NUM: Maximum number of inference events(100/200/500/1000/...)

Execute command parameter example:
command /Users/xxx/project/src/main/resources/ wikipedia LP 3 1500
```

### 4.2 Dataset

[Wikipedia and Reddit](http://snap.stanford.edu/jodie/#datasets)

### 4.3 GNN Model

An example of a model in the path（`./src/main/resources/model/`）.
