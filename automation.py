from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import asdict
from models import *
from topic_manager import KafkaManager
from user_manager import KafkaUserManager
from models import KafkaConfig

class KafkaAutomation:
    """Kafka 自動化管理類，整合 Topic 和使用者管理功能"""

    def __init__(self, kafka_config: 'KafkaConfig'):
        """
        初始化 Kafka 自動化管理器
        
        Args:
            kafka_config: Kafka 配置對象
        """
        self.config = kafka_config
        self.topic_manager = KafkaManager(kafka_config)
        self.user_manager = KafkaUserManager(kafka_config)
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """設置日誌記錄器"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def create_kafka_resources(
        self,
        topic_config: 'TopicConfig',
        user_configs: Optional[List['UserConfig']] = None,
        acl_configs: Optional[List['AclConfig']] = None
    ) -> Dict[str, Any]:
        """
        創建 Kafka 資源（Topic、使用者和 ACL）
        
        Args:
            topic_config: Topic 配置
            user_configs: 使用者配置列表（可選）
            acl_configs: ACL 配置列表（可選）
        
        Returns:
            Dict[str, Any]: 操作結果
        """
        results = {
            'success': True,
            'topic_created': False,
            'users_created': [],
            'acls_created': [],
            'errors': []
        }

        try:
            # 檢查並創建 Topic
            if self.topic_manager.check_topic_exists(topic_config.name):
                results['errors'].append(f"Topic {topic_config.name} 已存在")
            else:
                topic_success = self.topic_manager.create_topic(topic_config)
                results['topic_created'] = topic_success
                if not topic_success:
                    results['errors'].append(f"創建 Topic {topic_config.name} 失敗")
                    results['success'] = False
                    return results

            # 創建使用者和 ACL（如果提供）
            if user_configs:
                for user_config in user_configs:
                    if self.user_manager.check_user_exists(user_config.username):
                        results['errors'].append(f"使用者 {user_config.username} 已存在")
                        continue
                    
                    user_success = self.user_manager.create_user(user_config)
                    if user_success:
                        results['users_created'].append(user_config.username)
                    else:
                        results['errors'].append(
                            f"創建使用者 {user_config.username} 失敗"
                        )
                        results['success'] = False

            if acl_configs:
                for acl_config in acl_configs:
                    if not self.user_manager.check_user_exists(acl_config.username):
                        results['errors'].append(
                            f"無法創建 ACL: 使用者 {acl_config.username} 不存在"
                        )
                        continue
                    
                    acl_success = self.user_manager.create_acl(acl_config)
                    if acl_success:
                        results['acls_created'].append(
                            f"{acl_config.username}:{acl_config.topic_name}"
                        )
                    else:
                        results['errors'].append(
                            f"為使用者 {acl_config.username} 創建 ACL 失敗"
                        )
                        results['success'] = False

            return results

        except Exception as e:
            self.logger.error(f"創建 Kafka 資源時發生錯誤: {str(e)}")
            results['success'] = False
            results['errors'].append(str(e))
            return results

    def delete_kafka_resources(
        self,
        topic_name: str,
        delete_users: bool = False,
        usernames: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        刪除 Kafka 資源
        
        Args:
            topic_name: Topic 名稱
            delete_users: 是否同時刪除關聯的使用者
            usernames: 要刪除的特定使用者列表（如果 delete_users 為 True）
        
        Returns:
            Dict[str, Any]: 操作結果
        """
        results = {
            'success': True,
            'topic_deleted': False,
            'users_deleted': [],
            'errors': []
        }

        try:
            # 刪除 Topic
            if self.topic_manager.check_topic_exists(topic_name):
                topic_success = self.topic_manager.delete_topic(topic_name)
                results['topic_deleted'] = topic_success
                if not topic_success:
                    results['errors'].append(f"刪除 Topic {topic_name} 失敗")
                    results['success'] = False
            else:
                results['errors'].append(f"Topic {topic_name} 不存在")

            # 刪除使用者（如果需要）
            if delete_users:
                users_to_delete = usernames if usernames else []
                if not users_to_delete:
                    # 獲取與該 Topic 相關的所有使用者
                    acls = self.user_manager.list_acls()
                    users_to_delete = list(set(
                        acl['principal'].split(':')[1]
                        for acl in acls
                        if acl['resource_type'] == 'TOPIC'
                        and acl['resource_name'] == topic_name
                    ))

                for username in users_to_delete:
                    if self.user_manager.check_user_exists(username):
                        user_success = self.user_manager.delete_user(username)
                        if user_success:
                            results['users_deleted'].append(username)
                        else:
                            results['errors'].append(
                                f"刪除使用者 {username} 失敗"
                            )
                            results['success'] = False
                    else:
                        results['errors'].append(f"使用者 {username} 不存在")

            return results

        except Exception as e:
            self.logger.error(f"刪除 Kafka 資源時發生錯誤: {str(e)}")
            results['success'] = False
            results['errors'].append(str(e))
            return results

    async def check_resources_status(
        self,
        topic_name: Optional[str] = None,
        username: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        檢查資源狀態
        
        Args:
            topic_name: Topic 名稱（可選）
            username: 使用者名稱（可選）
        
        Returns:
            Dict[str, Any]: 資源狀態信息
        """
        status = {
            'cluster_health': True,
            'topic_status': None,
            'user_status': None,
            'errors': []
        }

        try:
            # 檢查集群狀態
            cluster_info = self.topic_manager.get_cluster_info()
            if not cluster_info:
                status['cluster_health'] = False
                status['errors'].append("無法獲取集群信息")
                return status

            status['cluster_info'] = cluster_info

            # 檢查 Topic 狀態
            if topic_name:
                topic_details = self.topic_manager.get_topic_details(topic_name)
                if topic_details:
                    topic_stats = self.topic_manager.get_topic_statistics(topic_name)
                    status['topic_status'] = {
                        'exists': True,
                        'details': topic_details,
                        'statistics': topic_stats,
                        'health': (
                            topic_stats
                            and not topic_stats['has_configuration_issues']
                            and topic_stats['under_replicated_partitions'] == 0
                        )
                    }
                else:
                    status['topic_status'] = {'exists': False}
                    status['errors'].append(f"Topic {topic_name} 不存在")

            # 檢查使用者狀態
            if username:
                user_exists = self.user_manager.check_user_exists(username)
                if user_exists:
                    acls = self.user_manager.list_acls(username)
                    status['user_status'] = {
                        'exists': True,
                        'acls': acls,
                        'has_permissions': len(acls) > 0
                    }
                else:
                    status['user_status'] = {'exists': False}
                    status['errors'].append(f"使用者 {username} 不存在")

            return status

        except Exception as e:
            self.logger.error(f"檢查資源狀態時發生錯誤: {str(e)}")
            status['cluster_health'] = False
            status['errors'].append(str(e))
            return status

    def bulk_resource_management(
        self,
        operations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        批量資源管理操作
        
        Args:
            operations: 操作列表，每個操作包含 'action' 和相關配置
        
        Returns:
            Dict[str, Any]: 批量操作結果
        """
        results = {
            'success': True,
            'completed_operations': [],
            'failed_operations': [],
            'errors': []
        }

        try:
            for op in operations:
                op_result = {
                    'action': op.get('action'),
                    'success': False,
                    'details': None
                }

                try:
                    if op['action'] == 'create_topic':
                        success = self.topic_manager.create_topic(op['topic_config'])
                        op_result['success'] = success
                        op_result['details'] = {
                            'topic': op['topic_config'].name
                        }

                    elif op['action'] == 'create_user':
                        success = self.user_manager.create_user(op['user_config'])
                        op_result['success'] = success
                        op_result['details'] = {
                            'username': op['user_config'].username
                        }

                    elif op['action'] == 'create_acl':
                        success = self.user_manager.create_acl(op['acl_config'])
                        op_result['success'] = success
                        op_result['details'] = {
                            'username': op['acl_config'].username,
                            'topic': op['acl_config'].topic_name
                        }

                    elif op['action'] == 'delete_topic':
                        success = self.topic_manager.delete_topic(op['topic_name'])
                        op_result['success'] = success
                        op_result['details'] = {
                            'topic': op['topic_name']
                        }

                    elif op['action'] == 'delete_user':
                        success = self.user_manager.delete_user(op['username'])
                        op_result['success'] = success
                        op_result['details'] = {
                            'username': op['username']
                        }

                    else:
                        raise ValueError(f"未知的操作類型: {op['action']}")

                    if op_result['success']:
                        results['completed_operations'].append(op_result)
                    else:
                        results['failed_operations'].append(op_result)
                        results['success'] = False

                except Exception as e:
                    op_result['error'] = str(e)
                    results['failed_operations'].append(op_result)
                    results['success'] = False
                    results['errors'].append(
                        f"操作 {op['action']} 失敗: {str(e)}"
                    )

            return results

        except Exception as e:
            self.logger.error(f"批量資源管理時發生錯誤: {str(e)}")
            results['success'] = False
            results['errors'].append(str(e))
            return results