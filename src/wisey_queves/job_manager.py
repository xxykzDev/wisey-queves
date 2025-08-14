"""
Job manager for handling async operations with state management
"""
import os
import json
import logging
import hashlib
import asyncio
from typing import Optional, Dict, Any, Set
from datetime import datetime, timedelta
import redis.asyncio as redis
from .job_models import (
    JobStatus, JobRequest, JobResult, JobUpdate,
    JobPartialResult
)
from .producer import BaseKafkaProducer
from wisey_telemetry.telemetry import start_trace_span

logger = logging.getLogger(__name__)


class JobManager:
    """Manages job lifecycle and state transitions"""
    
    def __init__(
        self,
        redis_client: redis.Redis,
        kafka_producer: Optional[BaseKafkaProducer] = None,
        service_name: str = "job-manager"
    ):
        self.redis = redis_client
        self.producer = kafka_producer
        self.service_name = service_name
        self.idempotency_cache: Dict[str, str] = {}
        self._cleanup_task = None
        
    async def start(self):
        """Start the job manager"""
        if self.producer:
            await self.producer.start()
        # Start cleanup task for expired jobs
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_jobs())
        logger.info("🚀 Job Manager started")
    
    async def stop(self):
        """Stop the job manager"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self.producer:
            await self.producer.stop()
        logger.info("🛑 Job Manager stopped")
    
    async def create_job(
        self,
        request: JobRequest,
        idempotency_key: Optional[str] = None
    ) -> JobRequest:
        """Create a new job with idempotency support"""
        with start_trace_span("job.create", {
            "job.type": request.job_type.value,
            "job.id": request.job_id,
        }) as span:
            
            # Check idempotency
            if idempotency_key:
                existing_job_id = await self._check_idempotency(idempotency_key)
                if existing_job_id:
                    # Return existing job
                    existing_job = await self.get_job(existing_job_id)
                    if existing_job:
                        logger.info(f"♻️ Returning existing job {existing_job_id} for idempotency key {idempotency_key}")
                        span.set_attribute("job.idempotent", True)
                        return existing_job
                
                # Store idempotency key
                await self._store_idempotency(idempotency_key, request.job_id)
                request.idempotency_key = idempotency_key
            
            # Store job in Redis
            job_key = f"job:{request.job_id}"
            job_data = request.model_dump_json()
            
            # Set with TTL of 24 hours
            await self.redis.setex(job_key, 86400, job_data)
            
            # Add to job index for this user/tenant
            if request.user_id:
                await self.redis.sadd(f"user:{request.user_id}:jobs", request.job_id)
                await self.redis.expire(f"user:{request.user_id}:jobs", 86400)
            
            # Send to Kafka for processing
            if self.producer:
                await self.producer.send(
                    topic=f"job.{request.job_type.value}",
                    value=request.model_dump(),
                    key=request.job_id
                )
            
            logger.info(f"📋 Job created: {request.job_id} ({request.job_type.value})")
            span.set_attribute("job.created", True)
            
            return request
    
    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        progress: int = 0,
        message: Optional[str] = None
    ) -> bool:
        """Update job status and notify subscribers"""
        with start_trace_span("job.update", {
            "job.id": job_id,
            "job.status": status.value,
            "job.progress": progress,
        }):
            
            job = await self.get_job(job_id)
            if not job:
                logger.error(f"❌ Job not found: {job_id}")
                return False
            
            # Update job status
            job.status = status
            job.updated_at = datetime.utcnow()
            
            # Store updated job
            job_key = f"job:{job_id}"
            await self.redis.setex(job_key, 86400, job.model_dump_json())
            
            # Create update event
            update = JobUpdate(
                job_id=job_id,
                status=status,
                progress=progress,
                message=message
            )
            
            # Publish update to Redis pubsub for WebSocket gateway
            await self.redis.publish(
                f"job.updates",
                update.model_dump_json()
            )
            
            # Also store latest status for polling fallback
            status_key = f"job:{job_id}:status"
            await self.redis.setex(status_key, 3600, update.model_dump_json())
            
            logger.info(f"📊 Job {job_id} updated: {status.value} ({progress}%)")
            return True
    
    async def store_job_result(
        self,
        job_id: str,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = 86400
    ) -> JobResult:
        """Store job result with checksum and versioning"""
        with start_trace_span("job.store_result", {
            "job.id": job_id,
            "job.has_error": error is not None,
        }):
            
            # Calculate checksum
            checksum = None
            if result:
                result_json = json.dumps(result, sort_keys=True)
                checksum = hashlib.sha256(result_json.encode()).hexdigest()
            
            # Get current version
            version_key = f"job:{job_id}:version"
            current_version = await self.redis.get(version_key)
            version = int(current_version) + 1 if current_version else 1
            
            # Create result object
            job_result = JobResult(
                job_id=job_id,
                status=JobStatus.FAILED if error else JobStatus.COMPLETED,
                result=result,
                error=error,
                checksum=checksum,
                version=version,
                ttl_seconds=ttl_seconds
            )
            
            # Store result
            result_key = f"job:{job_id}:result"
            await self.redis.setex(
                result_key,
                ttl_seconds,
                job_result.model_dump_json()
            )
            
            # Update version
            await self.redis.setex(version_key, ttl_seconds, str(version))
            
            # Update job status
            await self.update_job_status(
                job_id,
                JobStatus.FAILED if error else JobStatus.COMPLETED,
                progress=100
            )
            
            # Publish completion event
            await self.redis.publish(
                f"job.completions",
                job_result.model_dump_json()
            )
            
            logger.info(f"✅ Job {job_id} result stored (v{version}, checksum: {checksum})")
            return job_result
    
    async def store_partial_result(
        self,
        job_id: str,
        sequence: int,
        chunk: Dict[str, Any]
    ) -> bool:
        """Store partial result for streaming"""
        partial = JobPartialResult(
            job_id=job_id,
            sequence=sequence,
            chunk=chunk
        )
        
        # Store in sorted set by sequence
        partial_key = f"job:{job_id}:partials"
        await self.redis.zadd(
            partial_key,
            {partial.model_dump_json(): sequence}
        )
        await self.redis.expire(partial_key, 3600)
        
        # Publish for real-time streaming
        await self.redis.publish(
            f"job.partials",
            partial.model_dump_json()
        )
        
        logger.debug(f"📦 Partial result stored for job {job_id} (seq: {sequence})")
        return True
    
    async def get_job(self, job_id: str) -> Optional[JobRequest]:
        """Get job by ID"""
        job_key = f"job:{job_id}"
        job_data = await self.redis.get(job_key)
        
        if job_data:
            return JobRequest.model_validate_json(job_data)
        return None
    
    async def get_job_result(self, job_id: str) -> Optional[JobResult]:
        """Get job result"""
        result_key = f"job:{job_id}:result"
        result_data = await self.redis.get(result_key)
        
        if result_data:
            return JobResult.model_validate_json(result_data)
        return None
    
    async def get_job_status(self, job_id: str) -> Optional[JobUpdate]:
        """Get latest job status for polling"""
        status_key = f"job:{job_id}:status"
        status_data = await self.redis.get(status_key)
        
        if status_data:
            return JobUpdate.model_validate_json(status_data)
        
        # Fallback to job data
        job = await self.get_job(job_id)
        if job:
            return JobUpdate(
                job_id=job_id,
                status=job.status,
                progress=0 if job.status == JobStatus.QUEUED else 50
            )
        return None
    
    async def get_partial_results(
        self,
        job_id: str,
        start_sequence: int = 0
    ) -> list[JobPartialResult]:
        """Get partial results starting from sequence"""
        partial_key = f"job:{job_id}:partials"
        
        # Get all partials with score >= start_sequence
        partials_data = await self.redis.zrangebyscore(
            partial_key,
            start_sequence,
            "+inf"
        )
        
        partials = []
        for data in partials_data:
            partials.append(JobPartialResult.model_validate_json(data))
        
        return sorted(partials, key=lambda p: p.sequence)
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        job = await self.get_job(job_id)
        if not job:
            return False
        
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            logger.warning(f"⚠️ Job {job_id} already in terminal state: {job.status}")
            return False
        
        await self.update_job_status(job_id, JobStatus.CANCELLED, progress=0)
        
        # Send cancellation event to workers
        if self.producer:
            await self.producer.send(
                topic="job.cancellations",
                value={"job_id": job_id},
                key=job_id
            )
        
        logger.info(f"🚫 Job {job_id} cancelled")
        return True
    
    async def get_user_jobs(
        self,
        user_id: str,
        status_filter: Optional[JobStatus] = None
    ) -> list[JobRequest]:
        """Get all jobs for a user"""
        job_ids = await self.redis.smembers(f"user:{user_id}:jobs")
        
        jobs = []
        for job_id in job_ids:
            job = await self.get_job(job_id.decode() if isinstance(job_id, bytes) else job_id)
            if job:
                if not status_filter or job.status == status_filter:
                    jobs.append(job)
        
        return sorted(jobs, key=lambda j: j.created_at, reverse=True)
    
    async def _check_idempotency(self, key: str) -> Optional[str]:
        """Check if idempotency key exists"""
        idem_key = f"idempotency:{key}"
        job_id = await self.redis.get(idem_key)
        return job_id.decode() if job_id else None
    
    async def _store_idempotency(self, key: str, job_id: str):
        """Store idempotency key with TTL"""
        idem_key = f"idempotency:{key}"
        # Store for 24 hours
        await self.redis.setex(idem_key, 86400, job_id)
    
    async def _cleanup_expired_jobs(self):
        """Background task to clean up expired jobs"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                # Find and expire old jobs
                cutoff = datetime.utcnow() - timedelta(days=3)
                
                # This would need to scan all jobs - in production use a sorted set by timestamp
                logger.info("🧹 Running job cleanup task")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in cleanup task: {e}")
                await asyncio.sleep(60)