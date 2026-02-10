"""
MongoDB数据库连接
"""
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
from config import settings


class MongoDB:
    """MongoDB连接管理类"""

    client: Optional[AsyncIOMotorClient] = None
    database = None

    def connect(self):
        """建立数据库连接"""
        if not self.client:
            self.client = AsyncIOMotorClient(settings.MONGODB_URL)
            self.database = self.client[settings.MONGODB_DATABASE]
            print(f"Connected to MongoDB: {settings.MONGODB_URL}")

    def close(self):
        """关闭数据库连接"""
        if self.client:
            self.client.close()
            self.client = None
            print("Closed MongoDB connection")

    def get_collection(self, collection_name: str = None):
        """获取集合"""
        if self.database is None:
            self.connect()
        name = collection_name or settings.MONGODB_COLLECTION
        return self.database[name]


# 全局数据库实例
mongodb = MongoDB()


async def get_db():
    """依赖注入：获取数据库集合"""
    return mongodb.get_collection()
