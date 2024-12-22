from typing import Optional, Dict, Any
import logging
from contextlib import contextmanager
from confluent_kafka.admin import AdminClient, KafkaException
from dataclasses import asdict


class KafkaBaseManager:
    """Kafka 管理的基礎類別，提供共用功能"""
    
    def __init__(self, kafka_config: 'KafkaConfig'):
        """
        初始化 Kafka 基礎管理器
        
        Args:
            kafka_config: Kafka 配置對象
        """
        self.config = kafka_config
        self.config.validate()  # 驗證配置
        self._admin_client: Optional[AdminClient] = None
        self.logger = self._setup_logger()

    def __del__(self):
        """析構函數，確保 AdminClient 被正確關閉"""
        self._close_admin_client()

    def _setup_logger(self) -> logging.Logger:
        """
        設置日誌記錄器
        
        Returns:
            logging.Logger: 配置完成的日誌記錄器
        """
        logger_name = f"{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        
        return logger

    def _create_admin_client(self) -> AdminClient:
        """
        創建 AdminClient 實例
        
        Returns:
            AdminClient: 新創建的 AdminClient 實例
            
        Raises:
            KafkaException: 當創建 AdminClient 失敗時
        """
        try:
            client_config = self.config.to_dict()
            return AdminClient(client_config)
        except Exception as e:
            self.logger.error(f"創建 AdminClient 失敗: {str(e)}")
            raise KafkaException(f"無法創建 AdminClient: {str(e)}")

    def _close_admin_client(self) -> None:
        """安全地釋放 AdminClient 引用"""
        if self._admin_client is not None:
            self._admin_client = None
            self.logger.debug("AdminClient 引用已釋放")

    @property
    def admin_client(self) -> AdminClient:
        """
        獲取或創建 AdminClient 的屬性
        
        Returns:
            AdminClient: 現有或新創建的 AdminClient 實例
        """
        if self._admin_client is None:
            self._admin_client = self._create_admin_client()
        return self._admin_client

    @contextmanager
    def get_admin_client(self):
        """
        使用 context manager 管理 AdminClient 的生命週期
        
        Yields:
            AdminClient: AdminClient 實例
            
        Examples:
            >>> with self.get_admin_client() as admin:
            ...     admin.list_topics()
        """
        try:
            yield self.admin_client
        finally:
            self._close_admin_client()

    def handle_operation_result(self, futures: Dict[Any, Any], operation_name: str) -> bool:
        """
        處理 Kafka 操作的異步結果
        
        Args:
            futures: Kafka 操作返回的 futures 字典
            operation_name: 操作名稱，用於日誌記錄
            
        Returns:
            bool: 操作是否成功
        """
        try:
            for future in futures.values():
                future.result()  # 等待操作完成
            self.logger.info(f"{operation_name} 操作成功完成")
            return True
        except Exception as e:
            self.logger.error(f"{operation_name} 操作失敗: {str(e)}")
            return False

    def check_connection(self) -> bool:
        """
        檢查與 Kafka 集群的連接狀態
        
        Returns:
            bool: 連接是否正常
        """
        try:
            with self.get_admin_client() as admin:
                admin.list_topics(timeout=5)
            self.logger.info("Kafka 集群連接正常")
            return True
        except Exception as e:
            self.logger.error(f"Kafka 集群連接失敗: {str(e)}")
            return False