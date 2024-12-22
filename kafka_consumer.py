
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import sys
import logging
from typing import Dict, Any

def setup_logger():
    """設置日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

class MessageConsumer:
    def __init__(self, configs: Dict[str, Any]):
        """
        初始化消費者
        
        Args:
            configs: Kafka 消費者配置
        """
        self.logger = setup_logger()
        self.consumer = Consumer(configs)
        self.running = True
    
    def consume_messages(self, topics: list):
        """
        消費訊息的主要方法
        
        Args:
            topics: 要訂閱的主題列表
        """
        try:
            # 訂閱主題
            self.consumer.subscribe(topics)
            self.logger.info(f"已訂閱主題: {topics}")

            while self.running:
                # 輪詢訊息
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.info(f'Reached end of partition: {msg.topic()} [{msg.partition()}]')
                    else:
                        self.logger.error(f'Error while consuming message: {msg.error()}')
                else:
                    # 處理接收到的訊息
                    try:
                        value = json.loads(msg.value().decode('utf-8'))
                        self.logger.info(f"收到訊息: Topic={msg.topic()}, Partition={msg.partition()}, "
                                       f"Offset={msg.offset()}, Value={value}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"JSON 解析錯誤: {e}")
                        continue

        except KeyboardInterrupt:
            self.logger.info("使用者中斷程式")
        except Exception as e:
            self.logger.error(f"發生錯誤: {e}")
        finally:
            self.close()

    def close(self):
        """關閉消費者"""
        try:
            self.consumer.close()
            self.logger.info("消費者已關閉")
        except Exception as e:
            self.logger.error(f"關閉消費者時發生錯誤: {e}")

def main():
    # 消費者配置
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test_group_1',  # 消費者群組 ID
        'auto.offset.reset': 'earliest',  # 從最早的訊息開始消費
        'enable.auto.commit': True
    }

    # 要訂閱的主題
    topics = ['test-topic_batch']

    # 創建並啟動消費者
    consumer = MessageConsumer(config)
    consumer.consume_messages(topics)

if __name__ == "__main__":
    main()