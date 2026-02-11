"""
MinIO对象存储服务
"""
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from config import settings
from typing import Optional
import uuid


class MinIOStorage:
    """MinIO存储管理类"""

    _client: Optional[Minio] = None

    @classmethod
    def get_client(cls) -> Minio:
        """获取MinIO客户端单例"""
        if cls._client is None:
            cls._client = Minio(
                endpoint=settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE
            )
            # 确保bucket存在
            cls._ensure_bucket()
        return cls._client

    @classmethod
    def _ensure_bucket(cls):
        """确保bucket存在，不存在则创建"""
        client = cls._client
        if not client:
            return
        try:
            if not client.bucket_exists(settings.MINIO_BUCKET_NAME):
                client.make_bucket(settings.MINIO_BUCKET_NAME)
                print(f"Created MinIO bucket: {settings.MINIO_BUCKET_NAME}")
            else:
                print(f"MinIO bucket exists: {settings.MINIO_BUCKET_NAME}")
        except Exception as e:
            print(f"MinIO bucket/connection error: {e}")

    @classmethod
    def upload_file(
        cls,
        file_data: bytes,
        file_name: str,
        content_type: str = "application/octet-stream",
        use_original_name: bool = False
    ) -> tuple[str, float]:
        """
        上传文件到MinIO

        Args:
            file_data: 文件二进制数据
            file_name: 原始文件名
            content_type: 文件MIME类型
            use_original_name: 是否使用原始文件名（默认使用UUID）

        Returns:
            (file_url, file_size): 文件URL和文件大小
        """
        client = cls.get_client()

        # 根据参数决定使用原始文件名还是生成唯一文件名
        if use_original_name:
            object_name = file_name
        else:
            ext = file_name.rsplit(".", 1)[-1] if "." in file_name else ""
            object_name = f"{uuid.uuid4()}.{ext}" if ext else str(uuid.uuid4())

        # 上传文件
        client.put_object(
            bucket_name=settings.MINIO_BUCKET_NAME,
            object_name=object_name,
            data=BytesIO(file_data),
            length=len(file_data),
            content_type=content_type
        )

        # 构建访问URL
        file_url = cls._build_file_url(object_name)

        return file_url, float(len(file_data))

    @classmethod
    def _build_file_url(cls, object_name: str) -> str:
        """构建文件访问URL"""
        protocol = "https" if settings.MINIO_SECURE else "http"
        return f"{protocol}://{settings.MINIO_ENDPOINT}/{settings.MINIO_BUCKET_NAME}/{object_name}"

    @classmethod
    def delete_file(cls, object_name: str) -> bool:
        """
        从MinIO删除文件

        Args:
            object_name: 对象名称

        Returns:
            是否删除成功
        """
        try:
            client = cls.get_client()
            client.remove_object(settings.MINIO_BUCKET_NAME, object_name)
            return True
        except S3Error:
            return False


# 全局存储实例
storage = MinIOStorage()
