"""
FastAPI主应用 - 非结构化数据解析系统
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from typing import List, Any
from datetime import datetime
from bson import ObjectId

from config import settings
from database import mongodb, get_db
from models import (
    ApiResponse,
    FileUploadResponse,
    ParseDataCreate,
    ParseDataListItem,
    ParseDataDetail,
    MessageResponse
)
from storage import MinIOStorage


# 创建FastAPI应用
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="用于保存非结构化解析数据的系统，支持文件上传和数据管理"
)


@app.on_event("startup")
async def startup_db_client():
    """应用启动时建立连接"""
    mongodb.connect()
    try:
        MinIOStorage.get_client()
        print("MinIO connected successfully")
    except Exception as e:
        print(f"Warning: MinIO connection failed - {e}")
        print("App will start but file upload will not work")


@app.on_event("shutdown")
async def shutdown_db_client():
    """应用关闭时断开连接"""
    mongodb.close()


# ==================== 接口1: 上传文件到MinIO ====================

@app.post(
    "/api/v1/upload",
    response_model=ApiResponse[FileUploadResponse],
    summary="上传文件到MinIO",
    description="接收图片或PDF等文件，上传到MinIO对象存储，返回访问URL"
)
async def upload_file(
    file: UploadFile = File(..., description="要上传的文件（图片、PDF等）")
):
    """
    上传文件到MinIO

    - **file**: 支持的文件类型包括图片（jpg、png、gif等）和PDF文档
    - 返回MinIO访问URL、文件名、bucket名称和文件大小
    """
    try:
        # 读取文件数据
        file_data = await file.read()

        # 确定content type
        content_type = file.content_type or "application/octet-stream"

        # 上传到MinIO
        file_url, file_size = MinIOStorage.upload_file(
            file_data=file_data,
            file_name=file.filename,
            content_type=content_type
        )

        return ApiResponse[FileUploadResponse](
            code=200,
            data=FileUploadResponse(
                url=file_url,
                file_name=file.filename,
                bucket=settings.MINIO_BUCKET_NAME,
                size=file_size
            ),
            message="success"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"文件上传失败: {str(e)}"
        )


# ==================== 接口2: 创建解析记录 ====================

@app.post(
    "/api/v1/parsed",
    response_model=ApiResponse[ParseDataDetail],
    summary="创建解析记录",
    description="创建新的解析数据记录，保存文件信息和解析后的JSON数据"
)
async def create_parse_data(
    data: ParseDataCreate,
    db=Depends(get_db)
):
    """
    创建解析记录

    - **name**: 文件名称
    - **size**: 文件大小
    - **minio_url**: MinIO文件URL（可选）
    - **json**: 解析后的JSON数据
    """
    try:
        # 准备文档数据
        now = datetime.now()
        document = {
            "name": data.name,
            "size": data.size,
            "minio_url": data.minio_url,
            "json": data.json,
            "created_at": now,
            "updated_at": now
        }
        print(document)
        # 插入数据库
        result = await db.insert_one(document)
        inserted_id = str(result.inserted_id)

        # 返回创建的记录
        return ApiResponse[ParseDataDetail](
            code=200,
            data=ParseDataDetail(
                id=inserted_id,
                name=data.name,
                size=data.size,
                minio_url=data.minio_url,
                json=data.json,
                created_at=now,
                updated_at=now
            ),
            message="success"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建解析记录失败: {str(e)}"
        )


# ==================== 接口3: 查询解析列表 ====================

@app.get(
    "/api/v1/parsed",
    response_model=ApiResponse[List[ParseDataListItem]],
    summary="查询解析列表",
    description="获取所有解析记录的列表，不包含JSON详情数据"
)
async def get_parse_data_list(
    skip: int = 0,
    limit: int = 100,
    db=Depends(get_db)
):
    """
    查询解析列表

    - **skip**: 跳过的记录数（分页）
    - **limit**: 返回的记录数限制
    - 返回列表不包含json字段，仅包含基本信息
    """
    try:
        # 构建投影（排除json字段）
        projection = {
            "json": 0
        }

        # 查询数据
        cursor = db.find(projection=projection).skip(skip).limit(limit)
        documents = await cursor.to_list(length=limit)

        # 转换ObjectId为字符串，使用id字段
        result = []
        for doc in documents:
            result.append(ParseDataListItem(
                id=str(doc["_id"]),
                name=doc["name"],
                size=doc["size"],
                minio_url=doc.get("minio_url"),
                created_at=doc["created_at"]
            ))

        return ApiResponse[List[ParseDataListItem]](
            code=200,
            data=result,
            message="success"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询解析列表失败: {str(e)}"
        )


# ==================== 接口4: 查询指定解析详情 ====================

@app.get(
    "/api/v1/parsed/{data_id}",
    response_model=ApiResponse[ParseDataDetail],
    summary="查询解析详情",
    description="通过ID查询指定解析记录的完整信息，包含JSON数据"
)
async def get_parse_data_detail(
    data_id: str,
    db=Depends(get_db)
):
    """
    查询解析详情

    - **data_id**: 解析记录的ID
    - 返回包含json字段的完整数据
    """
    try:
        # 验证ObjectId格式
        if not ObjectId.is_valid(data_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="无效的数据ID格式"
            )

        # 查询数据
        document = await db.find_one({"_id": ObjectId(data_id)})

        if not document:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到ID为 {data_id} 的解析记录"
            )

        # 转换为响应格式，使用id字段
        return ApiResponse[ParseDataDetail](
            code=200,
            data=ParseDataDetail(
                id=str(document["_id"]),
                name=document["name"],
                size=document["size"],
                minio_url=document.get("minio_url"),
                json=document["json"],
                created_at=document["created_at"],
                updated_at=document["updated_at"]
            ),
            message="success"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询解析详情失败: {str(e)}"
        )


# ==================== 接口5: 删除解析记录 ====================

@app.delete(
    "/api/v1/parsed/{data_id}",
    response_model=ApiResponse[MessageResponse],
    summary="删除解析记录",
    description="通过ID删除指定的解析记录"
)
async def delete_parse_data(
    data_id: str,
    db=Depends(get_db)
):
    """
    删除解析记录

    - **data_id**: 要删除的解析记录ID
    - 返回删除结果
    """
    try:
        # 验证ObjectId格式
        if not ObjectId.is_valid(data_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="无效的数据ID格式"
            )

        # 查询记录是否存在（获取minio_url以便清理文件）
        document = await db.find_one({"_id": ObjectId(data_id)})
        if not document:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到ID为 {data_id} 的解析记录"
            )

        # 删除数据库记录
        result = await db.delete_one({"_id": ObjectId(data_id)})

        if result.deleted_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"删除失败，未找到ID为 {data_id} 的解析记录"
            )

        # 可选：同时删除MinIO中的文件
        # if document.get("minio_url"):
        #     object_name = document["minio_url"].split("/")[-1]
        #     MinIOStorage.delete_file(object_name)

        return ApiResponse[MessageResponse](
            code=200,
            data=MessageResponse(
                message="解析记录删除成功",
                id=data_id
            ),
            message="success"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除解析记录失败: {str(e)}"
        )


# ==================== 根路径 ====================

@app.get("/", summary="根路径")
async def root():
    """API根路径，返回服务信息"""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "endpoints": {
            "upload": "POST /api/v1/upload",
            "create": "POST /api/v1/parsed",
            "list": "GET /api/v1/parsed",
            "detail": "GET /api/v1/parsed/{id}",
            "delete": "DELETE /api/v1/parsed/{id}"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True
    )
