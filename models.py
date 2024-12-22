from typing import Dict, List, Optional, Tuple
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType, NewTopic, KafkaException
from dataclasses import dataclass
import logging
from contextlib import contextmanager
from enum import Enum
from dataclasses import dataclass, field

class SaslMechanism(Enum):
    """SASL 機制枚舉"""
    PLAIN = "PLAIN"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"

class SecurityProtocol(Enum):
    """安全協議枚舉"""
    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"

# @dataclass
# class KafkaConfig:
#     """Kafka 配置數據類"""
#     # Zookeeper 配置（主要配置）
#     zookeeper_connect: str
    
#     # Kafka 配置（可選）
#     bootstrap_servers: str = "localhost:9092"  # 修改這行，設置預設值
#     security_protocol: Optional[SecurityProtocol] = None #= SecurityProtocol.PLAINTEXT  # 也設置一個預設值
#     sasl_mechanism: Optional[SaslMechanism] = None
#     sasl_username: Optional[str] = None
#     sasl_password: Optional[str] = None
    
#     def validate(self) -> None:
#         """驗證配置的有效性"""
#         if not self.zookeeper_connect:
#             raise ValueError("zookeeper_connect 不能為空")
            
#         # 如果有設置 bootstrap_servers，則檢查相關的安全配置
#         if self.bootstrap_servers:
#             if self.security_protocol in [SecurityProtocol.SASL_SSL, SecurityProtocol.SASL_PLAINTEXT]:
#                 if not all([self.sasl_mechanism, self.sasl_username, self.sasl_password]):
#                     raise ValueError("使用 SASL 時必須提供所有 SASL 相關配置")

#     def to_dict(self) -> Dict[str, str]:
#         """轉換為 AdminClient 配置字典"""
#         config = {
#             'bootstrap.servers': self.bootstrap_servers  # 總是包含 bootstrap.servers
#         }
        
#         if self.security_protocol:
#             config['security.protocol'] = self.security_protocol.value
        
#         if self.sasl_mechanism:
#             config.update({
#                 'sasl.mechanism': self.sasl_mechanism.value,
#                 'sasl.username': self.sasl_username,
#                 'sasl.password': self.sasl_password,
#             })
        
#         return config

@dataclass
class KafkaConfig:
    """Kafka 配置數據類"""
    zookeeper_connect: str
    bootstrap_servers: str = "localhost:9092"
    
    def to_dict(self) -> Dict[str, str]:
        """轉換為 AdminClient 配置字典"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'kafka-admin-client',
            'metadata.max.age.ms': '1000',
            'socket.timeout.ms': '30000',
            'request.timeout.ms': '30000'
        }
        return config

    def validate(self) -> None:
        """驗證配置的有效性"""
        if not self.zookeeper_connect:
            raise ValueError("zookeeper_connect 不能為空")
        if not self.bootstrap_servers:
            raise ValueError("bootstrap_servers 不能為空")
        
@dataclass
class UserConfig:
    """使用者配置數據類"""
    username: str
    password: str
    mechanism: SaslMechanism = SaslMechanism.SCRAM_SHA_256
    iterations: int = 4092  # 添加 iterations 參數
    
    def validate(self) -> None:
        """驗證使用者配置"""
        if not self.username or not self.password:
            raise ValueError("使用者名稱和密碼不能為空")
        if len(self.password) < 8:
            raise ValueError("密碼長度必須至少為 8 個字符")

@dataclass
class TopicConfig:
    """Topic 配置數據類"""
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = field(default_factory=dict)
    
    def validate(self) -> None:
        """驗證 Topic 配置"""
        if not self.name:
            raise ValueError("Topic 名稱不能為空")
        if self.num_partitions < 1:
            raise ValueError("分區數必須大於 0")
        if self.replication_factor < 1:
            raise ValueError("複製因子必須大於 0")

@dataclass
class AclConfig:
    """ACL 配置數據類"""
    username: str
    topic_name: str
    consumer_group: str
    operations: List[str] = field(default_factory=lambda: ["READ", "WRITE"])
    
    def validate(self) -> None:
        """驗證 ACL 配置"""
        if not self.username or not self.topic_name or not self.consumer_group:
            raise ValueError("使用者名稱、Topic 名稱和消費者群組不能為空")
        
        valid_operations = ["READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE"]
        invalid_ops = [op for op in self.operations if op not in valid_operations]
        if invalid_ops:
            raise ValueError(f"無效的操作類型: {invalid_ops}. 有效的操作類型為: {valid_operations}")