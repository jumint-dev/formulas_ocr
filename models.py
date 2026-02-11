"""
数据模型定义
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Any, Generic, TypeVar, Union
from datetime import datetime
from bson import ObjectId
import json


T = TypeVar('T')


class PyObjectId(ObjectId):
    """Pydantic兼容的ObjectId类型"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, handler=None):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type="string")


class ApiResponse(BaseModel, Generic[T]):
    """通用API响应包装"""
    code: int = Field(..., description="状态码")
    data: Optional[T] = Field(None, description="数据内容")
    message: str = Field(..., description="描述消息")


class FileUploadResponse(BaseModel):
    """文件上传响应"""
    url: str
    file_name: str
    bucket: str
    size: float


class ParseDataCreate(BaseModel):
    """创建解析数据请求"""
    name: str = Field(..., description="文件名称")
    size: float = Field(..., gt=0, description="文件大小")
    minio_url: Optional[str] = Field(None, description="MinIO文件URL")
    json: Any = Field(..., description="解析后的JSON数据")


class ParseDataListItem(BaseModel):
    """解析数据列表项（不包含JSON详情）"""
    id: str = Field(description="数据ID")
    name: str = Field(description="文件名称")
    size: float = Field(description="文件大小")
    minio_url: Optional[str] = Field(None, description="MinIO文件URL")
    created_at: datetime = Field(description="创建时间")


class ParseDataDetail(BaseModel):
    """解析数据详情（包含所有数据）"""
    id: str = Field(description="数据ID")
    name: str = Field(description="文件名称")
    size: float = Field(description="文件大小")
    minio_url: Optional[str] = Field(None, description="MinIO文件URL")
    json: Any = Field(description="解析后的JSON数据")
    created_at: datetime = Field(description="创建时间")
    updated_at: datetime = Field(description="更新时间")


class MessageResponse(BaseModel):
    """通用消息响应"""
    message: str
    id: Optional[str] = None


class ParseDataDB(ParseDataDetail):
    """数据库存储模型"""
    pass


class ImageInfo(BaseModel):
    """图片信息"""
    name: str = Field(description="图片文件名")
    path: str = Field(description="ZIP中的路径")
    url: str = Field(description="MinIO访问URL")


class MinerUExtractResult(BaseModel):
    """MinerU解析结果"""
    batch_id: str = Field(description="批次ID")
    data_id: str = Field(description="数据ID")
    file_name: str = Field(description="文件名")
    file_url: str = Field(description="MinIO文件URL")
    state: str = Field(description="状态: done/processing/failed")
    err_msg: str = Field(default="", description="错误信息")
    content_list: Optional[Any] = Field(None, description="content_list_v2.json内容")
    image_urls: list[ImageInfo] = Field(default_factory=list, description="上传的图片列表")
