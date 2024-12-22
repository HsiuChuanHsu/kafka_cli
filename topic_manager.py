from typing import Dict, List, Optional
from confluent_kafka.admin import (
    NewTopic, ConfigResource, ResourceType, KafkaError
)
from base_manager import KafkaBaseManager

class KafkaManager(KafkaBaseManager):
    """Kafka Topics 管理類"""

    def create_topic(self, topic_config: 'TopicConfig') -> bool:
        """
        創建 Kafka Topic
        
        Args:
            topic_config: Topic 配置對象
        
        Returns:
            bool: 是否創建成功
        """
        try:
            topic_config.validate()
            
            # 檢查 Topic 是否已存在
            if self.check_topic_exists(topic_config.name):
                self.logger.warning(f"Topic {topic_config.name} 已存在")
                return False

            new_topic = NewTopic(
                topic_config.name,
                num_partitions=topic_config.num_partitions,
                replication_factor=topic_config.replication_factor,
                config=topic_config.config
            )
            
            with self.get_admin_client() as admin:
                futures = admin.create_topics([new_topic])
                success = self.handle_operation_result(
                    futures,
                    f"創建 Topic {topic_config.name}"
                )
                
                # 如果有額外配置且創建成功，更新配置
                if success and topic_config.config:
                    return self.update_topic_config(
                        topic_config.name,
                        topic_config.config
                    )
                return success

        except Exception as e:
            self.logger.error(f"創建 Topic 時發生錯誤: {str(e)}")
            return False

    def delete_topic(self, topic_name: str) -> bool:
        """
        刪除 Kafka Topic
        
        Args:
            topic_name: Topic 名稱
        
        Returns:
            bool: 是否刪除成功
        """
        try:
            # 檢查 Topic 是否存在
            if not self.check_topic_exists(topic_name):
                self.logger.warning(f"Topic {topic_name} 不存在")
                return False

            with self.get_admin_client() as admin:
                futures = admin.delete_topics([topic_name])
                return self.handle_operation_result(
                    futures,
                    f"刪除 Topic {topic_name}"
                )

        except Exception as e:
            self.logger.error(f"刪除 Topic 時發生錯誤: {str(e)}")
            return False

    def list_topics(self, include_internal: bool = False) -> List[str]:
        """
        列出所有 Topics
        
        Args:
            include_internal: 是否包含內部 Topics
        
        Returns:
            List[str]: Topic 名稱列表
        """
        try:
            with self.get_admin_client() as admin:
                metadata = admin.list_topics(timeout=10)
                topics = []
                
                for topic_name in metadata.topics.keys():
                    if include_internal or not topic_name.startswith('_'):
                        topics.append(topic_name)
                
                self.logger.info(f"獲取到 {len(topics)} 個 Topics")
                return sorted(topics)

        except Exception as e:
            self.logger.error(f"列出 Topics 時發生錯誤: {str(e)}")
            return []

    def get_topic_config(self, topic_name: str) -> Optional[Dict[str, str]]:
        """
        獲取 Topic 配置
        
        Args:
            topic_name: Topic 名稱
        
        Returns:
            Optional[Dict[str, str]]: Topic 配置或 None（如果發生錯誤）
        """
        try:
            if not self.check_topic_exists(topic_name):
                self.logger.warning(f"Topic {topic_name} 不存在")
                return None

            resource = ConfigResource(
                ResourceType.TOPIC,
                topic_name
            )
            
            with self.get_admin_client() as admin:
                futures = admin.describe_configs([resource])
                
                for res, future in futures.items():
                    try:
                        configs = future.result()
                        return {
                            config.name: config.value
                            for config in configs.values()
                            if not config.is_default
                        }
                    except Exception as e:
                        self.logger.error(f"獲取 Topic 配置失敗: {str(e)}")
                        return None

        except Exception as e:
            self.logger.error(f"獲取 Topic 配置時發生錯誤: {str(e)}")
            return None

    def update_topic_config(
        self,
        topic_name: str,
        config_updates: Dict[str, str]
    ) -> bool:
        """
        更新 Topic 配置
        
        Args:
            topic_name: Topic 名稱
            config_updates: 要更新的配置
        
        Returns:
            bool: 是否更新成功
        """
        try:
            if not self.check_topic_exists(topic_name):
                self.logger.warning(f"Topic {topic_name} 不存在")
                return False

            resource = ConfigResource(
                ResourceType.TOPIC,
                topic_name,
                set_config=config_updates
            )
            
            with self.get_admin_client() as admin:
                futures = admin.alter_configs([resource])
                return self.handle_operation_result(
                    futures,
                    f"更新 Topic {topic_name} 配置"
                )

        except Exception as e:
            self.logger.error(f"更新 Topic 配置時發生錯誤: {str(e)}")
            return False

    def check_topic_exists(self, topic_name: str) -> bool:
        """
        檢查 Topic 是否存在
        
        Args:
            topic_name: Topic 名稱
            
        Returns:
            bool: Topic 是否存在
        """
        try:
            with self.get_admin_client() as admin:
                metadata = admin.list_topics(timeout=10)
                exists = topic_name in metadata.topics
                self.logger.debug(f"Topic {topic_name} {'存在' if exists else '不存在'}")
                return exists

        except Exception as e:
            self.logger.error(f"檢查 Topic 存在性時發生錯誤: {str(e)}")
            return False

    def get_topic_details(self, topic_name: str) -> Optional[Dict]:
        """
        獲取 Topic 詳細信息
        
        Args:
            topic_name: Topic 名稱
            
        Returns:
            Optional[Dict]: Topic 詳細信息或 None（如果發生錯誤）
        """
        try:
            if not self.check_topic_exists(topic_name):
                self.logger.warning(f"Topic {topic_name} 不存在")
                return None

            with self.get_admin_client() as admin:
                # 獲取 Topic 元數據
                metadata = admin.list_topics(topic=topic_name)
                topic_metadata = metadata.topics[topic_name]
                
                # 獲取 Topic 配置
                configs = self.get_topic_config(topic_name)
                
                # 獲取分區細節
                partitions = []
                for partition_id, partition in topic_metadata.partitions.items():
                    partitions.append({
                        'id': partition_id,
                        'leader': partition.leader,
                        'replicas': partition.replicas,
                        'isrs': partition.isrs
                    })
                
                return {
                    'name': topic_name,
                    'partitions': partitions,
                    'num_partitions': len(topic_metadata.partitions),
                    'replication_factor': len(topic_metadata.partitions[0].replicas),
                    'configs': configs or {},
                    'error': topic_metadata.error.str() if topic_metadata.error else None
                }

        except Exception as e:
            self.logger.error(f"獲取 Topic 詳細信息時發生錯誤: {str(e)}")
            return None

    def get_topic_statistics(self, topic_name: str) -> Optional[Dict]:
        """
        獲取 Topic 統計信息
        
        Args:
            topic_name: Topic 名稱
            
        Returns:
            Optional[Dict]: Topic 統計信息或 None（如果發生錯誤）
        """
        try:
            details = self.get_topic_details(topic_name)
            if not details:
                return None

            # 計算統計信息
            stats = {
                'name': topic_name,
                'num_partitions': details['num_partitions'],
                'replication_factor': details['replication_factor'],
                'total_replicas': sum(len(p['replicas']) for p in details['partitions']),
                'total_in_sync_replicas': sum(len(p['isrs']) for p in details['partitions']),
                'under_replicated_partitions': sum(
                    1 for p in details['partitions']
                    if len(p['isrs']) < len(p['replicas'])
                ),
                'has_configuration_issues': any(
                    len(p['isrs']) < details['replication_factor']
                    for p in details['partitions']
                )
            }

            # 添加重要配置
            important_configs = ['cleanup.policy', 'retention.ms', 'retention.bytes']
            stats['key_configs'] = {
                k: v for k, v in (details['configs'] or {}).items()
                if k in important_configs
            }

            return stats

        except Exception as e:
            self.logger.error(f"獲取 Topic 統計信息時發生錯誤: {str(e)}")
            return None

    def get_cluster_info(self) -> Optional[Dict]:
        """
        獲取 Kafka 集群信息
        
        Returns:
            Optional[Dict]: 集群信息或 None（如果發生錯誤）
        """
        try:
            with self.get_admin_client() as admin:
                metadata = admin.list_topics()
                
                topics = self.list_topics(include_internal=True)
                brokers = []
                
                for broker_id, broker in metadata.brokers.items():
                    brokers.append({
                        'id': broker_id,
                        'host': broker.host,
                        'port': broker.port
                    })
                
                return {
                    'broker_count': len(metadata.brokers),
                    'topic_count': len(topics),
                    'brokers': brokers,
                    'controller_id': metadata.controller_id
                }

        except Exception as e:
            self.logger.error(f"獲取集群信息時發生錯誤: {str(e)}")
            return None