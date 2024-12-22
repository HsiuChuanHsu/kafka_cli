from models import TopicConfig
from topic_manager import KafkaManager
from models import KafkaConfig

# 創建配置
config = KafkaConfig(
    zookeeper_connect="localhost:2181",
    bootstrap_servers="localhost:9092"
)

# 創建 topic manager
topic_manager = KafkaManager(config)

# 創建測試 topic
test_topic = TopicConfig(
    name="test-topic",
    num_partitions=1,
    replication_factor=1
)

# 創建 topic
result = topic_manager.create_topic(test_topic)
if result:
    print("Topic 創建成功！")
else:
    print("Topic 創建失敗。")