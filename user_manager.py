from typing import List, Dict, Optional
from confluent_kafka.admin import (
    ConfigResource,
    ResourceType,
    AclBinding,
    AclOperation,
    AclPermissionType,
    ResourcePatternType,
    AclBindingFilter,
    UserScramCredentialUpsertion,
    ScramMechanism,
    ScramCredentialInfo,
    AdminClient
)
from base_manager import KafkaBaseManager

class KafkaUserManager(KafkaBaseManager):
    """Kafka 使用者管理類"""

    def create_user(self, user_config: 'UserConfig') -> bool:
        """
        創建 Kafka 使用者使用 SCRAM 認證
        """
        try:
            self.logger.info(f"開始創建使用者: {user_config.username}")
            user_config.validate()

            # 建立 ScramCredentialInfo 物件
            scram_credential_info = ScramCredentialInfo(
                ScramMechanism.SCRAM_SHA_256,  # 指定 SCRAM 機制
                4096  # 設定迭代次數
            )

            # 使用 UserScramCredentialUpsertion 設定密碼
            credential_upsertion = UserScramCredentialUpsertion(
                user_config.username,
                scram_credential_info,  # 傳遞 ScramCredentialInfo 物件
                user_config.password.encode(),  # 密碼需轉換為 bytes
            )

            with self.get_admin_client() as admin:
                try:
                    futures = admin.alter_user_scram_credentials([credential_upsertion])

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
                    # 使用 AclBindingFilter 匹配所有使用者
                    acl_filter = AclBindingFilter(
                    restype=ResourceType.ANY,
                    name=None,
                    resource_pattern_type=ResourcePatternType.ANY,
                    principal=None,
                    host=None,
                    operation=AclOperation.ANY,
                    permission_type=AclPermissionType.ANY,
                    )

                    users = set()
                    future = admin.describe_acls(acl_filter)
                    result = future.result()
                    # 從 ACL 條目中提取使用者名稱
                    for acl in result:
                        principal = acl.principal
                        if principal.startswith("User:"):
                            users.add(principal[5:])

                    user_list = sorted(list(users))
                    self.logger.info(f"找到 {len(user_list)} 個使用者")
                    return user_list

                except Exception as e:
                    self.logger.error(f"獲取使用者列表時發生錯誤: {str(e)}")
                    return []

        except Exception as e:
            self.logger.error(f"列出使用者時發生錯誤: {str(e)}")
            return []


    def create_acl(self, admin_client: AdminClient, principal: str, operations: list, topic: str):
        """
        建立Kafka ACL。

        參數:
            admin_client: AdminClient實例
            principal: 允許訪問的主體，格式為"User:username"
            operations: 允許的操作列表，例如 ["Read", "Write"]
            topic: 主題名稱
        """
        acl_bindings = []
        for operation in operations:
            acl_binding = AclBinding(
                restype=ResourceType.TOPIC,
                name=topic,
                resource_pattern_type=ResourcePatternType.LITERAL,
                principal=principal,
                host='*',  # 允許所有主機
                operation=AclOperation.__dict__[operation.upper()],
                permission_type=AclPermissionType.ALLOW,
            )
            acl_bindings.append(acl_binding)

        # 建立ACL
        futures = admin_client.create_acls(acl_bindings)

        # 等待結果
        for future in futures.values():
            try:
                future.result()
                print(f"成功建立ACL: {acl_binding}")
            except Exception as e:
                print(f"建立ACL失敗: {e}")

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