from confluent_kafka.admin import AdminClient
from models import KafkaConfig, UserConfig
from user_manager import KafkaUserManager
import logging

def setup_logger() -> logging.Logger:
    """設置基本的日誌配置"""
    logger = logging.getLogger("ACLCreation")
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def main():
    logger = setup_logger()
    
    try:
        # 建立 KafkaConfig
        kafka_config = KafkaConfig(
            zookeeper_connect="localhost:2181",
            bootstrap_servers="localhost:9092"
        )
        
        # 建立 KafkaUserManager
        manager = KafkaUserManager(kafka_config)
        
        # 檢查連線狀態
        if not manager.check_connection():
            logger.error("無法連接到 Kafka 集群")
            return
            
        # 定義要設置 ACL 的使用者和主題
        username = "testuser3"
        topics_and_permissions = [
            ("test-topic_batch", ["READ", "WRITE"]),
            ("test-topic", ["READ", "WRITE"])
        ]
        
        # 檢查使用者是否存在
        if not manager.check_user_exists(username):
            logger.error(f"使用者 {username} 不存在")
            return
            
        # 使用 context manager 建立 ACLs
        with manager.get_admin_client() as admin_client:
            for topic, operations in topics_and_permissions:
                try:
                    manager.create_acl(
                        admin_client,
                        f"User:{username}",
                        operations,
                        topic
                    )
                    logger.info(f"成功為使用者 {username} 建立 {topic} 的 ACL 權限")
                except Exception as e:
                    logger.error(f"建立 {topic} 的 ACL 權限時發生錯誤: {str(e)}")
        
        logger.info("ACL 建立程序完成")

    except Exception as e:
        logger.error(f"執行過程中發生錯誤: {str(e)}")

if __name__ == "__main__":
    main()