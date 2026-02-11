"""
配置文件 - 集中管理系统配置
"""
from pydantic_settings import BaseSettings
from urllib.parse import quote_plus


class Settings(BaseSettings):
    """系统配置"""

    # FastAPI配置
    APP_NAME: str = "非结构化数据解析系统"
    APP_VERSION: str = "1.0.0"
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # MongoDB配置
    MONGODB_HOST: str = "172.21.0.3"
    MONGODB_PORT: int = 37017
    MONGODB_USER: str = "root"
    MONGODB_PASSWORD: str = "formulas"
    MONGODB_DATABASE: str = "ocr_parser"
    MONGODB_COLLECTION: str = "parser_list"

    @property
    def MONGODB_URL(self) -> str:
        """构建MongoDB连接URL"""
        if self.MONGODB_USER and self.MONGODB_PASSWORD:
            encoded_user = quote_plus(self.MONGODB_USER)
            encoded_pass = quote_plus(self.MONGODB_PASSWORD)
            return f"mongodb://{encoded_user}:{encoded_pass}@{self.MONGODB_HOST}:{self.MONGODB_PORT}"
        return f"mongodb://{self.MONGODB_HOST}:{self.MONGODB_PORT}"

    # MinIO配置
    MINIO_ENDPOINT: str = "172.21.0.93:9000"
    MINIO_ACCESS_KEY: str = "superadmin"
    MINIO_SECRET_KEY: str = "123.spsspro.com"
    MINIO_BUCKET_NAME: str = "oct-parser"
    MINIO_SECURE: bool = False

    # MinerU配置
    MINERU_API_URL: str = "https://mineru.net/api/v4"
    MINERU_API_KEY: str = "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFM1MTIifQ.eyJqdGkiOiI0MzgwMDYxMyIsInJvbCI6IlJPTEVfUkVHSVNURVIiLCJpc3MiOiJPcGVuWExhYiIsImlhdCI6MTc3MDYwNzM0NiwiY2xpZW50SWQiOiJsa3pkeDU3bnZ5MjJqa3BxOXgydyIsInBob25lIjoiIiwib3BlbklkIjpudWxsLCJ1dWlkIjoiZmZmYzlmMDktYzJiYy00MzA3LTk5MDQtN2M1ODQ2ZjY4NmU3IiwiZW1haWwiOiIiLCJleHAiOjE3NzE4MTY5NDZ9.FX3J3dmsKLyuCAFSFsdQqLEQqIhoVv4VtSjpBL98eP0O25C_Xhttoq_C76F7BFewfB9hdjlVGmJbpziuDgc07w"  # 需要在.env中配置API密钥

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
