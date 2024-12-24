from models import KafkaConfig, UserConfig, SaslMechanism
from user_manager import KafkaUserManager
import logging
import sys

# 設置詳細的日誌格式
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def main():
    try:
        # 使用外部端口 9092
        config = KafkaConfig(
            zookeeper_connect="localhost:2181",
            bootstrap_servers="localhost:9092"
        )

        user_manager = KafkaUserManager(config)

        # 創建測試使用者
        test_user = UserConfig(
            username="testuser3",
            password="TestPassword123",
            iterations=4096
        )

        print("正在檢查 Kafka 集群連接...")
        if not user_manager.check_connection():
            print("無法連接到 Kafka 集群")
            return

        print(f"正在檢查用戶 {test_user.username} 是否存在...")
        if user_manager.check_user_exists(test_user.username):
            print(f"用戶 {test_user.username} 已存在")
            return

        print(f"開始創建用戶 {test_user.username}...")
        result = user_manager.create_user(test_user)
        if result:
            print(f"使用者 {test_user.username} 創建成功！")
            
            # 驗證用戶創建
            if user_manager.check_user_exists(test_user.username):
                print(f"已驗證用戶 {test_user.username} 創建成功")
            else:
                print(f"警告：無法驗證用戶 {test_user.username} 是否創建成功")
        else:
            print(f"使用者 {test_user.username} 創建失敗。")

    except Exception as e:
        print(f"發生錯誤: {str(e)}")
        logging.exception("詳細錯誤信息：")

if __name__ == "__main__":
    main()