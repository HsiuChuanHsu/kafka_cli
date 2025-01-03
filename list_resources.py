from typing import Dict, List
from models import KafkaConfig
from user_manager import KafkaUserManager
from topic_manager import KafkaManager
from automation import KafkaAutomation
from contextlib import contextmanager
from confluent_kafka.admin import AdminClient
import logging
from tabulate import tabulate

class KafkaResourceLister:
    """Kafka 資源列表工具"""
    
    def __init__(self, zk_connect: str):
        """
        初始化資源列表工具
        
        Args:
            zk_connect: Zookeeper 連接字串
        """
        self.config = KafkaConfig(
            zookeeper_connect=zk_connect,
            bootstrap_servers="localhost:9092"  # 添加這行
        )
        self.topic_manager = KafkaManager(self.config)
        self.user_manager = KafkaUserManager(self.config)
        self.automation = KafkaAutomation(self.config)
        self.logger = self._setup_logger()
        self._admin_client = None

    def _setup_logger(self) -> logging.Logger:
        """設置日誌"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    @contextmanager
    def get_admin_client(self):
        """獲取 AdminClient 的 context manager"""
        try:
            admin_client = AdminClient({
                'bootstrap.servers': self.config.bootstrap_servers
            })
            yield admin_client
        finally:
            # AdminClient 不需要顯式關閉
            pass

    def list_consumer_groups(self) -> List[Dict]:
        """
        列出所有消費者群組
        
        Returns:
            List[Dict]: 消費者群組信息列表
        """
        try:
            consumer_groups = []
            with self.get_admin_client() as admin:
                try:
                    # 獲取消費者群組列表
                    future = admin.list_consumer_groups()
                    result = future.result()
                    
                    # 只處理有效的群組
                    for group in result.valid:
                        # 跳過系統群組（以 _ 開頭的群組）
                        if group.group_id.startswith('_'):
                            continue
                        try:
                            group_info = {
                                'name': group.group_id,
                                'members': [],
                                'state': getattr(group, 'state', 'Unknown')
                            }
                            
                            # 獲取群組詳細信息
                            desc_future = admin.describe_consumer_groups([group.group_id])
                            for g_id, future in desc_future.items():
                                try:
                                    description = future.result()
                                    # 提取成員信息
                                    for member in description.members:
                                        group_info['members'].append({
                                            'id': member.member_id,
                                            'client_id': member.client_id,
                                            'host': member.host
                                        })
                                except Exception as e:
                                    self.logger.error(f"獲取群組 {g_id} 詳細信息時發生錯誤: {str(e)}")
                            
                            consumer_groups.append(group_info)
                            
                        except Exception as e:
                            self.logger.error(f"處理群組信息時發生錯誤: {str(e)}")

                except Exception as e:
                    self.logger.error(f"獲取消費者群組列表時發生錯誤: {str(e)}")
                    
                return consumer_groups

        except Exception as e:
            self.logger.error(f"列出消費者群組時發生錯誤: {str(e)}")
            return []
        

    def print_user_acls(self):
        """以表格方式打印使用者和 ACL 權限"""
        print("\n=== Users and ACLs ===")
        
        users = self.user_manager.list_users()
        for user in users:
            print(f"\nUser: {user}")
            acls = self.user_manager.list_acls(user)
            
            if acls:
                # 準備表格數據
                table_data = []
                for acl in acls:
                    table_data.append([
                        acl['resource_type'],
                        acl['resource_name'],
                        acl['operation'],
                        acl['permission']
                    ])
                
                # 使用 tabulate 打印表格
                headers = ['Resource Type', 'Resource Name', 'Operation', 'Permission']
                print(tabulate(table_data, headers=headers, tablefmt='grid'))
            else:
                print("No ACLs found")

    def print_all_resources(self):
        """打印所有資源信息"""
        # 打印 Topics
        print("\n=== Topics ===")
        topics = self.topic_manager.list_topics()
        topics_table = []
        for topic in topics:
            details = self.topic_manager.get_topic_details(topic)
            if details:
                topics_table.append([
                    topic,
                    details['num_partitions'],
                    details['replication_factor']
                ])
        
        print(tabulate(topics_table, 
                    headers=['Topic', 'Partitions', 'Replication Factor'],
                    tablefmt='grid'))

        # 打印使用者和 ACL (使用新的表格格式)
        self.print_user_acls()

        # 打印消費者群組
        print("\n=== Consumer Groups ===")
        groups = self.list_consumer_groups()
        groups_table = []
        for group in groups:
            for member in group.get('members', []):
                groups_table.append([
                    group['name'],
                    group.get('state', 'Unknown'),
                    member['client_id'],
                    member['host']
                ])
            if not group.get('members'):
                groups_table.append([
                    group['name'],
                    group.get('state', 'Unknown'),
                    '-',
                    '-'
                ])
        
        print(tabulate(groups_table,
                    headers=['Group', 'State', 'Client ID', 'Host'],
                    tablefmt='grid'))

def main():
    # Zookeeper 連接配置
    zk_connect = "localhost:2181"
    
    # 創建資源列表工具
    lister = KafkaResourceLister(zk_connect)
    
    # 打印所有資源
    lister.print_all_resources()

if __name__ == "__main__":
    main()