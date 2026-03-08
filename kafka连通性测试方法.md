# Kafka 连通性测试方法

本文档包含两部分：
1. 项目中关键源码文件与它们的作用说明（便于初学者理解代码结构）
2. 在 Windows（PowerShell）下逐步测试 Kafka 连通性的方法、常见问题排查与预期结果

---

## 一、项目关键文件说明（逐文件）

- `src/main/java/com/spb/socialplatformback/SocialPlatformBackApplication.java`
  - Spring Boot 应用入口（`main`），如果你使用 Spring Boot 启动整个应用，这个类负责引导 Spring 容器。

- `src/main/java/com/spb/socialplatformback/WeiboHotSearchJob.java`
  - Flink 作业的入口类（程序调度点），职责尽量轻量：配置 Flink 执行环境、做 Kafka Admin 的预检查（确保 topic 存在并有分区），创建 Kafka source，并把来源交给 pipeline 去处理；最后启动 Flink 作业并触发窗口聚合与输出。
  - 注意：我已把 Kafka source 与分析逻辑拆分，`WeiboHotSearchJob` 只负责 wiring（连接）和启动。

- `src/main/java/com/spb/socialplatformback/kafka/KafkaSourceFactory.java`
  - 一个小工厂类，封装创建 `KafkaSource<String>` 的逻辑（bootstrapServers、topic、groupId、起始偏移等）。目的是把 Kafka 客户端相关的代码封装，便于复用和测试。

- `src/main/java/com/spb/socialplatformback/pipeline/SentimentAnalysisPipeline.java`
  - 情感分析流水线的封装：把字符串流解析为 `HotSearchItem`（JSON -> DTO），并把 DTO 映射为 `SentimentResult`（调用 `SentimentAnalyzer`）。
  - 包含一个 `parseJson(String)` 的非 Flink 帮助方法，便于在单元测试中解析 JSON 而无需运行 Flink 本地环境。

- `src/main/java/com/spb/socialplatformback/analysis/SentimentAnalyzer.java`
  - 简单的情感分析器（依靠词典匹配）。这是一个轻量级的启始实现，适合原型或演示；生产中可替换为更复杂的模型或调用外部 NLP 服务。

- `src/main/java/com/spb/socialplatformback/analysis/SentimentResult.java`
  - 情感分析结果的 DTO，承载比如 id/url、score（-1/0/1）、label（NEGATIVE/NEUTRAL/POSITIVE）和时间戳。

- `src/main/java/com/spb/socialplatformback/dto/HotSearchItem.java`
  - 接收来自 Kafka 的 JSON 对应的 DTO（字段包括 rank, title, url, hot_count, first_crawled, source 等）。注意 JSON 字段通过 Jackson 注解映射到 Java 字段（例如 `hot_count` -> `hotCount`）。

- `src/test/java/...`
  - 包含一些单元测试：`SentimentAnalyzerTest`（情感得分基本用例）、`SentimentAnalysisPipelineTest`（JSON 解析的单元测试）。

- `pom.xml`
  - Maven 构建配置，列出依赖（Flink、Kafka connector、Jackson、JUnit 等）和插件（例如 Spring Boot 插件）。

- `src/main/resources/application.properties`
  - Spring Boot（或程序）配置文件，通常用于保存 Kafka 地址、数据库配置等可配置项（如果有你可以查看并根据需要修改）。

- `target/` 目录
  - Maven 构建产物（jar、class 文件等），运行 `mvn package` 会生成。

---

## 二、仅测试 Kafka 连通性（PowerShell）

下面是一个按步骤、针对 Windows PowerShell 的连通性检查手册（假设本地或远端已有 Kafka broker，默认端口 9092）：

前提：
- 你知道 Kafka 的 `bin` 目录（例如 Kafka 解压后的 `kafka_*/bin/windows`），或 Kafka 在容器中通过端口映射暴露了 9092。
- 如果 Kafka 在 Docker 中运行，确保 host 和容器之间的端口映射与 `advertised.listeners` 配置是一致且可达的。

步骤 1 — 检查端口是否可达（Broker 是否正在监听）
```powershell
# 用 PowerShell 测试 TCP 端口（本机示例）
Test-NetConnection -ComputerName localhost -Port 9092
```
- 期望：TcpTestSucceeded: True。如果为 False，说明 broker 未启动或端口被防火墙阻止。

步骤 2 — 列出 topics
```powershell
# 进入 Kafka 的 bin/windows 目录，然后运行：
.\kafka-topics.bat --bootstrap-server localhost:9092 --list
```
- 期望：列出已有的 topics（如果没有列出你会看到空结果）。如果报错连接失败，先回到步骤 1 排查。

步骤 3 — 描述某个 topic（查看分区信息）
```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic weibo_hotsearch
```
- 期望：看到 topic 的 partition 数量和 leader 节点等信息。注意：如果 topic 不存在会报错或没有输出。

步骤 4 — （如果 topic 不存在）创建一个测试 topic
```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic weibo_hotsearch
```
- 说明：本地测试通常使用 replication-factor=1。生产环境下请设置合适的副本数和分区数。

步骤 5 — 生产（发送）一条测试消息（JSON）
```powershell
# 下面示例会把一条 JSON 发送到 topic
echo '{"rank":1,"title":"测试标题 很好","url":"http://t","hot_count":100,"first_crawled":1700000000,"source":"weibo"}' | .\kafka-console-producer.bat --broker-list localhost:9092 --topic weibo_hotsearch
```
- 说明：生产成功通常没有太多输出，若报错则检查 broker 地址与 topic 是否正确。

步骤 6 — 用控制台消费者消费刚才的消息（验证消息能被读到）
```powershell
.\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic weibo_hotsearch --from-beginning --max-messages 5
```
- 期望：能看到你在步骤 5 发送的 JSON。若看不到，请检查消息是否确实发送到相同的 topic 和分区，以及消费者偏移位置。

步骤 7 — 可选：用 Kafka AdminClient 的 Java 程序检测（如果你更喜欢用程序化方式）
- 下面是一个最小 Java 代码示例，用于连接 Kafka 并列出 topics（用于排查与集成测试）：

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import java.util.Properties;

public class KafkaAdminCheck {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try (AdminClient admin = AdminClient.create(props)) {
            System.out.println("Topics: " + admin.listTopics().names().get());
        }
    }
}
```
- 编译并运行：
  - 推荐用 Maven/IDE 将 Kafka 客户端依赖加入到一个小项目中并运行，或将依赖加入你现有的项目里，使用 `mvn exec:java` 或直接在 IDE 里运行。

---

## 三、常见问题与排查指南

1. Rebootstrapping / 元数据不断重试（大量 AdminMetadataManager 日志）
   - 原因：客户端能连接到 bootstrap 端口，但 broker 返回的 advertised.listeners（broker 对外宣称的地址）是一个客户端不可达的地址（例如容器内部地址或主机名）。
   - 排查：检查 broker 的 `server.properties`（或 Docker Compose 环境变量），确认 `advertised.listeners` 指向客户端可达的地址/端口（例如 `PLAINTEXT://host-ip:9092`）。

2. 只能生产不能消费（或反之）
   - 检查 topic 是否在不同客户端下被创建在不同的集群（多个 Kafka 实例）；确保 `bootstrap.servers` 指向同一个 broker/集群。
   - 检查消费者组偏移：使用 `--from-beginning` 来确保从头读取做测试。

3. 如果 Kafka 在 Docker 中运行：
   - 确认容器端口映射：`-p 9092:9092`，以及 `advertised.listeners` 使用宿主 IP（不是容器名或 0.0.0.0）。
   - 若使用 Docker Compose，务必检查 environment 中的 `KAFKA_ADVERTISED_LISTENERS` 是否正确。

4. 防火墙/端口问题
   - 在 Windows 上用 `Test-NetConnection` 检查端口；在 Linux 上用 `nc -vz host 9092` 或 `telnet host 9092`。

5. 版本或协议不兼容
   - 确保 Kafka 客户端与 broker 版本兼容（通常小版本差异兼容，但极端老新版本间可能有协议差异）。

---

## 四、在本项目中快速验证（两个选项）

A) 使用系统自带的 Kafka shell 工具（最快）：按照上文步骤 1-6 在 Kafka 的 `bin/windows` 目录运行命令。

B) 使用本项目中的 Admin 预检查逻辑：
- 我们在 `WeiboHotSearchJob` 的 `main` 中加入了 AdminClient 的预检查（会在启动时去查询 topic 和 broker）。你可以直接运行该主类（在 IDE 中运行或用 `java -jar`），如果它打印出 `Topic '...' found with X partitions`，说明连通性正常。
- 运行示例（jar 包形式）：
```powershell
java -Dkafka.bootstrap.servers=localhost:9092 -Dkafka.topic=weibo_hotsearch -jar target\social-platform-back-0.0.1-SNAPSHOT.jar
```
- 注意：如果你看到大量 `AdminMetadataManager -- Rebootstrapping` 日志，说明 Admin client 持��尝试获取元数据（回到常见问题第 1 点排查）。

---

## 五、期望的快速检查结果示例

- `Test-NetConnection` 返回 TcpTestSucceeded: True
- `kafka-topics.bat --list` 能列出你的 topic 或 `--describe` 能看到 partition/leader 信息
- 用 console-producer 发送消息后，console-consumer 能读到相同的 JSON
- 或运行 `WeiboHotSearchJob` 时，启动日志显示 `Topic 'weibo_hotsearch' found with 1 partitions`（或其他分区数）并继续进入消费阶段

---

如果你愿意，我可以：
- 帮你把 `WeiboHotSearchJob` 的默认 topic 值统一为 `weibo_hotsearch`（下划线）或任何你确定的名称；
- 在项目中添加一个小的 `kafka-healthcheck` 命令行类（可直接运行，返回 0/1），用于自动化检查与 CI。

需要我现在把哪一项做了？（例如：把默认 topic 改为下划线版，或添加 kafka-healthcheck 主类）

