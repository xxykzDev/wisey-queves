"""
Job models and state management for async operations
"""
from enum import Enum
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


class JobStatus(str, Enum):
    """Job lifecycle states"""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class JobType(str, Enum):
    """Types of async jobs"""
    FETCH_STORES = "fetch_stores"
    FETCH_PRODUCTS = "fetch_products"
    FETCH_CATEGORIES = "fetch_categories"
    FETCH_USER = "fetch_user"
    UPDATE_USER = "update_user"
    CREATE_STORE = "create_store"
    UPDATE_STORE = "update_store"


class JobRequest(BaseModel):
    """Job request model"""
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_type: JobType
    status: JobStatus = JobStatus.QUEUED
    idempotency_key: Optional[str] = None
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None


class JobUpdate(BaseModel):
    """Job progress update"""
    job_id: str
    status: JobStatus
    progress: int = Field(ge=0, le=100)
    message: Optional[str] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class JobPartialResult(BaseModel):
    """Partial result for streaming"""
    job_id: str
    sequence: int
    chunk: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class JobResult(BaseModel):
    """Final job result"""
    job_id: str
    status: JobStatus
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    checksum: Optional[str] = None
    version: int = 1
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    ttl_seconds: int = 86400  # 24 hours default


# WebSocket Event Models
class WSEventType(str, Enum):
    """WebSocket event types"""
    # Client -> Server
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PING = "ping"
    
    # Server -> Client
    JOB_UPDATE = "job.update"
    JOB_PARTIAL = "job.partial"
    JOB_DONE = "job.done"
    JOB_ERROR = "job.error"
    HEARTBEAT = "heartbeat"
    PONG = "pong"
    ACK = "ack"
    ERROR = "error"


class WSEvent(BaseModel):
    """Base WebSocket event"""
    type: WSEventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    schema_version: str = "jobs.v1"


class WSSubscribe(WSEvent):
    """Client subscription request"""
    type: WSEventType = WSEventType.SUBSCRIBE
    job_id: str
    resume_from: Optional[int] = None  # For resuming partial results


class WSJobUpdate(WSEvent):
    """Job status update event"""
    type: WSEventType = WSEventType.JOB_UPDATE
    job_id: str
    status: JobStatus
    progress: int = Field(ge=0, le=100)
    message: Optional[str] = None


class WSJobPartial(WSEvent):
    """Partial result event"""
    type: WSEventType = WSEventType.JOB_PARTIAL
    job_id: str
    sequence: int
    chunk: Dict[str, Any]


class WSJobDone(WSEvent):
    """Job completion event"""
    type: WSEventType = WSEventType.JOB_DONE
    job_id: str
    checksum: str
    version: int
    result_url: Optional[str] = None  # URL to fetch full result


class WSJobError(WSEvent):
    """Job error event"""
    type: WSEventType = WSEventType.JOB_ERROR
    job_id: str
    code: str
    message: str
    retry_after: Optional[int] = None  # Seconds to wait before retry


class WSHeartbeat(WSEvent):
    """Server heartbeat"""
    type: WSEventType = WSEventType.HEARTBEAT
    server_time: datetime = Field(default_factory=datetime.utcnow)
    active_connections: Optional[int] = None


class WSAck(WSEvent):
    """Acknowledgment event"""
    type: WSEventType = WSEventType.ACK
    ack_for: str  # Original event type being acknowledged
    job_id: Optional[str] = None
    success: bool = True
    message: Optional[str] = None