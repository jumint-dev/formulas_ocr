"""
FastAPI主应用 - 非结构化数据解析系统
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from typing import List, Any
from datetime import datetime
from bson import ObjectId
import requests
import tempfile
import os
import uuid
import zipfile
import io

from config import settings
from database import mongodb, get_db
from models import (
    ApiResponse,
    FileUploadResponse,
    ParseDataCreate,
    ParseDataListItem,
    ParseDataDetail,
    MessageResponse,
    MinerUExtractResult
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


# ==================== 接口1.5: MinerU文件解析 ====================

@app.post(
    "/api/v1/mineru/upload",
    response_model=ApiResponse[MinerUExtractResult],
    summary="上传文件到MinerU解析",
    description="接收文件，上传到MinerU服务进行解析，返回解析结果和content_list_v2.json内容"
)
async def upload_to_mineru(
    file: UploadFile = File(..., description="要上传的文件（图片、PDF等）")
):
    """
    上传文件到MinerU解析

    - **file**: 支持的文件类型包括图片和PDF文档
    - 返回解析结果和content_list_v2.json内容
    """
    temp_file_path = None
    minio_file_url = None
    try:
        # 1. 读取文件数据
        file_data = await file.read()

        # 2. 上传文件到MinIO
        content_type = file.content_type or "application/octet-stream"
        minio_file_url, file_size = MinIOStorage.upload_file(
            file_data=file_data,
            file_name=file.filename,
            content_type=content_type
        )
        print(f"MinIO upload success: {minio_file_url}")

        # 3. 保存到临时文件用于上传到MinerU
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename or "")[1]) as temp_file:
            temp_file.write(file_data)
            temp_file_path = temp_file.name

        # 4. 调用MinerU API获取上传URL
        headers = {}
        if settings.MINERU_API_KEY:
            headers["Authorization"] = f"Bearer {settings.MINERU_API_KEY}"

        # 生成唯一的data_id
        data_id = str(uuid.uuid4())
        print(data_id,'data_id')
        response = requests.post(
            f"{settings.MINERU_API_URL}/file-urls/batch",
            headers=headers,
            json={
                "files": [
                    {"name": file.filename, "data_id": data_id}
                ],
                "model_version": "vlm"
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()

        # 3. 检查响应并上传文件
        if result["code"] == 0:
            batch_id = result["data"]["batch_id"]
            urls = result["data"]["file_urls"]

            print(batch_id,'batch_id')
            if not urls:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="MinerU API未返回上传URL"
                )

            upload_url = urls[0]

            # 上传文件到MinerU
            with open(temp_file_path, 'rb') as f:
                print("testing upload to MinerU with URL:", upload_url)
                res_upload = requests.put(upload_url, data=f)
                print("MinerU upload response status:",res_upload.status_code)
                if res_upload.status_code != 200:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"文件上传到MinerU失败: {res_upload.status_code}"
                    )

            # 4. 轮询查询解析结果
            import time
            max_retries = 30  # 最多轮询30次
            retry_interval = 2  # 每次间隔2秒
            content_list = None
            image_urls = []
            final_state = "processing"
            final_err_msg = ""

            for attempt in range(max_retries):
                time.sleep(retry_interval)
                print(f"Polling attempt {attempt + 1}/{max_retries}...")

                result_response = requests.get(
                    f"{settings.MINERU_API_URL}/extract-results/batch/{batch_id}",
                    headers=headers,
                    timeout=60
                )
                result_response.raise_for_status()
                result_data = result_response.json()

                if result_data["code"] == 0:
                    extract_results = result_data["data"].get("extract_result", [])
                    if extract_results:
                        extract_info = extract_results[0]
                        final_state = extract_info.get("state", "unknown")
                        final_err_msg = extract_info.get("err_msg", "")

                        print(f"Current state: {final_state}")

                        if final_state == "done":
                            # 解析完成，下载zip并读取content_list_v2.json
                            if extract_info.get("full_zip_url"):
                                zip_url = extract_info["full_zip_url"]
                                print("Downloading result zip from:", zip_url)
                                zip_response = requests.get(zip_url, timeout=60)
                                print("Zip download response status:",zip_response.status_code)
                                zip_response.raise_for_status()

                                # 从内存中读取zip文件
                                with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zip_ref:
                                    # 查找layout.json文件
                                    for file_info in zip_ref.filelist:
                                        if "layout.json" in file_info.filename:
                                            # 读取JSON内容
                                            with zip_ref.open(file_info) as json_file:
                                                import json
                                                content_list = json.loads(json_file.read().decode('utf-8'))
                                            break

                                    # 上传images文件夹中的所有图片到MinIO
                                    image_urls = []
                                    for file_info in zip_ref.filelist:
                                        if file_info.filename.startswith("images/") and not file_info.is_dir():
                                            # 读取图片数据
                                            with zip_ref.open(file_info) as image_file:
                                                image_data = image_file.read()
                                                # 获取文件名（不含路径）
                                                image_name = os.path.basename(file_info.filename)
                                                print("image_name:", image_name)
                                                # 确定content type
                                                ext = os.path.splitext(image_name)[1].lower()
                                                content_type_map = {
                                                    '.jpg': 'image/jpeg',
                                                    '.jpeg': 'image/jpeg',
                                                    '.png': 'image/png',
                                                    '.gif': 'image/gif',
                                                    '.bmp': 'image/bmp',
                                                    '.webp': 'image/webp'
                                                }
                                                content_type = content_type_map.get(ext, 'application/octet-stream')
                                                # 上传到MinIO（使用原始文件名）
                                                image_url, _ = MinIOStorage.upload_file(
                                                    file_data=image_data,
                                                    file_name=image_name,
                                                    content_type=content_type,
                                                    use_original_name=True
                                                )
                                                image_urls.append({
                                                    "name": image_name,
                                                    "path": file_info.filename,
                                                    "url": image_url
                                                })
                                    print(image_urls)
                                    print(f"Uploaded {len(image_urls)} images to MinIO")
                            break
                        elif final_state == "failed":
                            # 解析失败
                            break
                        # 继续轮询

            return ApiResponse[MinerUExtractResult](
                code=200,
                data=MinerUExtractResult(
                    batch_id=batch_id,
                    data_id=data_id,
                    file_name=file.filename or "",
                    file_url=minio_file_url or "",
                    state=final_state,
                    err_msg=final_err_msg,
                    content_list=content_list,
                    image_urls=image_urls
                ),
                message="success"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"获取MinerU上传URL失败: {result.get('msg', '未知错误')}"
            )

    except requests.RequestException as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"MinerU API请求失败: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"MinerU文件上传失败: {str(e)}"
        )
    finally:
        # 清理临时文件
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception:
                pass


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
            "mineru_upload": "POST /api/v1/mineru/upload",
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
