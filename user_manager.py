from typing import List, Dict, Optional
from confluent_kafka.admin import (
    ConfigResource,
    ResourceType,
    AclBinding,
    AclOperation,
    AclPermissionType,
    ResourcePatternType,
    AclBindingFilter,
    UserScramCredentialAlteration, UserScramCredentialUpsertion,
    ScramMechanism, ScramCredentialInfo
)
from base_manager import KafkaBaseManager
import os

class KafkaUserManager(KafkaBaseManager):
    """Kafka 使用者管理類"""

    def create_user(self, user_config: 'UserConfig') -> bool:
        """
        創建 Kafka 使用者使用 SCRAM 認證
        """
        try:
            self.logger.info(f"開始創建使用者: {user_config.username}")
            user_config.validate()

            # 使用 ConfigResource 替代方案
            config = {
                'SCRAM-SHA-256': f'[password={user_config.password}]'
            }

            user_resource = ConfigResource(
                ResourceType.USER,
                user_config.username,
                set_config=config
            )

            with self.get_admin_client() as admin:
                try:
                    futures = admin.alter_configs([user_resource])
                    
                    # 等待操作完成
                    for future in futures.values():
                        future.result()
                    
                    self.logger.info(f"使用者 {user_config.username} 創建成功")
                    return True

                except Exception as e:
                    self.logger.error(f"創建使用者失敗: {str(e)}")
                    return False

        except Exception as e:
            self.logger.error(f"創建使用者時發生錯誤: {str(e)}")
            return False

    def check_user_exists(self, username: str) -> bool:
        """
        檢查使用者是否存在
        
        Args:
            username: 使用者名稱
            
        Returns:
            bool: 使用者是否存在
        """
        try:
            self.logger.debug(f"開始檢查使用者 {username} 是否存在")
            
            with self.get_admin_client() as admin:
                try:
                    futures = admin.describe_user_scram_credentials([username])
                    
                    for future in futures.values():
                        try:
                            future.result()
                            self.logger.debug(f"使用者 {username} 存在")
                            return True
                        except Exception as e:
                            if "RESOURCE_NOT_FOUND" in str(e) or "does not exist" in str(e):
                                self.logger.debug(f"使用者 {username} 不存在")
                                return False
                            raise
                    return False

                except Exception as e:
                    if "RESOURCE_NOT_FOUND" in str(e) or "does not exist" in str(e):
                        self.logger.debug(f"使用者 {username} 不存在")
                        return False
                    self.logger.error(f"檢查使用者存在性時發生錯誤: {str(e)}")
                    raise

        except Exception as e:
            if "RESOURCE_NOT_FOUND" in str(e) or "does not exist" in str(e):
                return False
            self.logger.error(f"檢查使用者存在性時發生錯誤: {str(e)}")
            return False

    def delete_user(self, username: str) -> bool:
        """
        刪除 Kafka 使用者
        
        Args:
            username: 使用者名稱
        
        Returns:
            bool: 是否刪除成功
        """
        try:
            # 檢查使用者是否存在
            if not self.check_user_exists(username):
                self.logger.warning(f"使用者 {username} 不存在")
                return False

            # 首先刪除該使用者的所有 ACL
            self.delete_user_acls(username)

            # 刪除使用者配置
            user_resource = ConfigResource(
                ResourceType.USER,
                username,
                delete_config=['SCRAM-SHA-256', 'SCRAM-SHA-512']
            )

            with self.get_admin_client() as admin:
                futures = admin.alter_configs([user_resource])
                return self.handle_operation_result(futures, "刪除使用者")

        except Exception as e:
            self.logger.error(f"刪除使用者時發生錯誤: {str(e)}")
            return False


    def list_users(self) -> List[str]:
        """
        列出所有使用者
        
        Returns:
            List[str]: 使用者名稱列表
        """
        try:
            with self.get_admin_client() as admin:
                try:
                    # 使用空字串作為使用者名來描述所有使用者
                    resource = ConfigResource(
                        ResourceType.USER,
                        name="*"  # 使用萬用字元
                    )
                    
                    users = set()
                    futures = admin.describe_configs([resource])
                    
                    for _, future in futures.items():
                        try:
                            result = future.result()
                            # 解析配置以獲取使用者名稱
                            for key in result.keys():
                                if '.SCRAM-SHA-' in key:
                                    username = key.split('.')[0]
                                    if username and username != '*':
                                        users.add(username)
                        except Exception as e:
                            if "Could not find" not in str(e):
                                self.logger.error(f"解析使用者配置時發生錯誤: {str(e)}")
                            continue
                    
                    user_list = sorted(list(users))
                    self.logger.info(f"找到 {len(user_list)} 個使用者")
                    return user_list
                    
                except Exception as e:
                    self.logger.error(f"獲取使用者列表時發生錯誤: {str(e)}")
                    return []
                    
        except Exception as e:
            self.logger.error(f"列出使用者時發生錯誤: {str(e)}")
            return []


    def create_acl(self, acl_config: 'AclConfig') -> bool:
        """
        創建 ACL 權限
        
        Args:
            acl_config: ACL 配置對象，包含 username, topic_name, consumer_group, operations
        
        Returns:
            bool: 是否創建成功
        """
        try:
            acl_config.validate()
            
            acl_bindings = []
            
            # Topic 權限設置
            if acl_config.topic_name:
                # 創建 READ 權限
                topic_binding = AclBinding(
                    pattern_type=ResourcePatternType.LITERAL,
                    resource_type=ResourceType.TOPIC,
                    resource_name=acl_config.topic_name,
                    principal=f"User:{acl_config.username}",
                    host="*",
                    operation=AclOperation.READ,
                    permission_type=AclPermissionType.ALLOW
                )
                acl_bindings.append(topic_binding)
                
                # 如果包含 WRITE 操作
                if "WRITE" in acl_config.operations:
                    write_binding = AclBinding(
                        pattern_type=ResourcePatternType.LITERAL,
                        resource_type=ResourceType.TOPIC,
                        resource_name=acl_config.topic_name,
                        principal=f"User:{acl_config.username}",
                        host="*",
                        operation=AclOperation.WRITE,
                        permission_type=AclPermissionType.ALLOW
                    )
                    acl_bindings.append(write_binding)

            # Consumer Group 權限設置
            if acl_config.consumer_group and "READ" in acl_config.operations:
                group_binding = AclBinding(
                    pattern_type=ResourcePatternType.LITERAL,
                    resource_type=ResourceType.GROUP,
                    resource_name=acl_config.consumer_group,
                    principal=f"User:{acl_config.username}",
                    host="*",
                    operation=AclOperation.READ,
                    permission_type=AclPermissionType.ALLOW
                )
                acl_bindings.append(group_binding)

            # 創建所有 ACL
            success = True
            with self.get_admin_client() as admin:
                futures = admin.create_acls(acl_bindings)
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"創建 ACL 失敗: {str(e)}")
                        success = False

            if success:
                self.logger.info(
                    f"成功為使用者 {acl_config.username} 創建 ACL "
                    f"(Topic: {acl_config.topic_name}, "
                    f"Consumer Group: {acl_config.consumer_group})"
                )
            return success

        except Exception as e:
            self.logger.error(f"創建 ACL 時發生錯誤: {str(e)}")
            return False

    def delete_user_acls(self, username: str) -> bool:
        """
        刪除使用者的所有 ACL 權限
        
        Args:
            username: 使用者名稱
        
        Returns:
            bool: 是否刪除成功
        """
        try:
            acls = self.list_acls(username)
            if not acls:
                return True

            with self.get_admin_client() as admin:
                # 創建刪除請求
                deletion_futures = []
                for acl in acls:
                    binding = AclBinding(
                        pattern_type=ResourcePatternType.LITERAL,
                        resource_type=ResourceType[acl['resource_type']],
                        resource_name=acl['resource_name'],
                        principal=acl['principal'],
                        host="*",
                        operation=AclOperation[acl['operation']],
                        permission_type=AclPermissionType[acl['permission']]
                    )
                    deletion_futures.extend(admin.delete_acls([binding]))

                return all(
                    self.handle_operation_result({f: f}, "刪除 ACL")
                    for f in deletion_futures
                )

        except Exception as e:
            self.logger.error(f"刪除使用者 ACL 時發生錯誤: {str(e)}")
            return False

    def list_acls(self, username: Optional[str] = None) -> List[Dict]:
        """
        列出 ACL 權限
        
        Args:
            username: 可選的使用者名稱過濾
            
        Returns:
            List[Dict]: ACL 權限列表
        """
        try:
            acls = []
            with self.get_admin_client() as admin:
                try:
                    # 建立 AclBindingFilter 來比對 ACL 繫結
                    acl_binding_filter = AclBindingFilter(
                        restype=ResourceType.ANY,
                        name=None,
                        resource_pattern_type=ResourcePatternType.ANY,
                        principal=f"User:{username}" if username else None,
                        host=None,
                        operation=AclOperation.ANY,
                        permission_type=AclPermissionType.ANY
                    )
                    
                    # 獲取單一 future
                    future = admin.describe_acls(acl_binding_filter)
                    
                    try:
                        # 直接取得結果
                        result = future.result()
                        # result 應該是一個 AclBinding 列表
                        for acl in result:
                            acls.append({
                                'resource_type': acl.restype.name,
                                'resource_name': acl.name,
                                'principal': acl.principal,
                                'operation': acl.operation.name,
                                'permission': acl.permission_type.name
                            })
                    except Exception as e:
                        if "no acls" not in str(e).lower():
                            self.logger.error(f"獲取 ACL 列表失敗: {str(e)}")
                except Exception as e:
                    if "no acls" not in str(e).lower():
                        self.logger.error(f"描述 ACL 時發生錯誤: {str(e)}")
            
            return acls

        except Exception as e:
            self.logger.error(f"列出 ACL 時發生錯誤: {str(e)}")
            return []